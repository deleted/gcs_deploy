#!/usr/bin/env python
import settings
import httplib
import hashlib
import os
import sys
import time
import subprocess
import argparse
import sqlite3
import multiprocessing, logging
import Queue

logger = multiprocessing.log_to_stderr()
logger.setLevel(multiprocessing.SUBDEBUG)

DB_TRANSACTION_SIZE = 100000 # for bulk commits and updates to the inventory database
global gsutil_path

def gsutil(*args):
    args = (gsutil_path,) + args
    print " ".join(args)
    return subprocess.check_call(args)

def md5_digest(filepath):
    md5 = hashlib.md5()
    with open(filepath, 'rb') as f:
        for bytes in iter(lambda: f.read(128*md5.block_size), ''):
            md5.update(bytes)
    return md5.hexdigest()

def sync_file(path, bucket, start_path, check_remote=True):
    logger.info( "SYNC " + path + "...")
    if start_path not in path:
        path = os.path.join(start_path, path)
    relpath = os.path.relpath(path, start_path)
    url = "http://commondatastorage.googleapis.com/%s/%s" % (bucket, relpath)
    if check_remote:
        conn = httplib.HTTPConnection(bucket+'.commondatastorage.googleapis.com')
        conn.request('HEAD', '/'+relpath)
        response = conn.getresponse() 
        if response.status == 200:
            remote_hash = response.getheader('etag').replace('"','')
            local_hash = md5_digest(path)
            if remote_hash == local_hash:
                print "OK"
                return
            else:
                raise Exception( "MD5 mismatch... %s :: %s" % (local_hash, remote_hash) )
    logger.debug("UPLOADING " + path)
    gsutil('-m', 'cp', path, "gs://"+bucket+"/"+relpath)
    logger.debug("UPLOADED " +path)

def sync_recursive(source_dir, dest_bucket):
    """ Sync files from source path to dest_bucket on GCS."""
    gsutil( "setdefacl", "public-read", "gs://"+dest_bucket) # make uploads world-readable
    print "%s --> gs://%s" % (source_dir, dest_bucket)
    for (dirpath, dirnames, filenames) in os.walk(source_dir):
        print len(filenames)
        for filename in filenames:
            sync_file(os.path.join(dirpath, filename), dest_bucket, source_dir)

def mark_complete(filepath, connection, dblock=None):
    if dblock: dblock.acquire()
    cursor = connection.cursor()
    logger.debug("UPDATING: %s" % filepath)
    cursor.execute('UPDATE files set remote_exists = 1 where path = ?', (filepath,))
    logger.debug("UPDATED: %d" % cursor.rowcount)
    assert cursor.rowcount == 1
    connection.commit()
    logger.debug("COMMITTED")
    time.sleep(0.2)
    if dblock: dblock.release()

def worker(task_q, result_q, finished, dbname):
    while True:
        if finished.is_set():
            break
        try:
            path, dest_bucket, start_path = task_q.get(False, 0.1) # retry after a timeout
        except Queue.Empty:
            continue
        sync_file(path, dest_bucket, start_path, check_remote=False)
        result_q.put(path)
        task_q.task_done()
    
def db_reporter(q, finished, dbname):
    connection = sqlite3.Connection(dbname)
    while True:
        if finished.is_set():
            break
        try:
            path = q.get(False, 0.1) # retry after a timeout
        except Queue.Empty:
            continue
        mark_complete(path, connection)
        q.task_done()


def sync_parallel(filename_iterator, options):
    response = None
    while response not in ('y','n'):
        response = raw_input("Do you want to run a remote inventory first?  You probably do. (y/n)")

    task_q = multiprocessing.JoinableQueue(options.max_queue_size)
    result_q = multiprocessing.JoinableQueue(options.max_queue_size)
    finished = multiprocessing.Event()
    subprocesses = [ multiprocessing.Process(target=worker, args=(task_q, result_q, finished, options.inventory_db)) for i in range(options.num_processes) ]
    reporter = multiprocessing.Process(target=db_reporter, args=(result_q, finished, options.inventory_db) )
    subprocesses.append(reporter)
    for p in subprocesses:
        p.start()
    for path in filename_iterator:
        task_q.put( (path, options.dest_bucket, options.source_dir) )
    task_q.join()
    result_q.join()
    finished.set()
    logger.info("Finished.  Waiting for subprocesses to join")
    for p in subprocesses:
        p.join()

def names_from_db(dbname, which="untransfered", dblock=None):
    global connection
    qry = "SELECT path FROM files"
    if which != "all":
        qry += " WHERE remote_exists IS NULL"
    qry += " LIMIT %d" % options.db_chunk_size
    print qry
    while True:
        cursor = connection.cursor()
        if dblock: dblock.acquire()
        cursor.execute(qry)
        if dblock: dblock.release()
        records = cursor.fetchall()
        if len(records) == 0: break
        for record in records:
            yield record[0]
        

def search_for_gsutil():
    # Search for gsutil executable in the $PATH
    for path in os.environ['PATH'].split(':'):
        testpath = os.path.join(path, 'gsutil')
        if os.path.exists(testpath):
            if os.access(testpath, os.X_OK):  # test executability
                return testpath
                break
            else:
                descend = os.path.join(testpath, 'gsutil')
                if os.path.exists(descend) and os.access(descend, os.X_OK):
                    return descend
                    break
    else:
        raise Exception("Couldn't find gsutil in your $PATH.  Please add it or specify it with the --gsutil-path option.")
    
def ensure_exists(path):
    try:
        os.mkdir(path)
    except OSError: #presumably directory already exists
        pass

def local_inventory(source_dir, bucketname):
    inventory_path = options.inventory_path
    ensure_exists(inventory_path)
    conn = sqlite3.connect(options.inventory_db)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS files;")
    cursor.execute("DROP INDEX IF EXISTS idx_path;")
    cursor.execute("""
    CREATE TABLE files (
        id INTEGER PRIMARY KEY,
        path TEXT NOT NULL,
        local_hash TEXT,
        remote_hash TEXT,
        remote_exists INTEGER
    );
    """)
    conn.commit()

    count = 0
    for (dirpath, dirnames, filenames) in os.walk(source_dir):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            relpath = os.path.relpath(filepath, source_dir)
            #filehash = md5_digest(filepath)
            #cursor.execute('INSERT INTO files (path, local_hash) VALUES (?, ?);',  (relpath, filehash))
            cursor.execute('INSERT INTO files (path) VALUES (?);',  (relpath, ))
            count += 1
            if count % DB_TRANSACTION_SIZE == 0:
                conn.commit()
        print "%s: %d inserts." % (dirpath, count)
    else:
        conn.commit()
    print "Creating index...",
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_path ON files(path);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_remote_exist ON files(remote_exists);')
    print "Done."

def remote_inventory(source_dir, bucketname, options):
    inventory_path = options.inventory_path 
    assert os.path.exists(options.inventory_db)

    listfile_name = os.path.join(inventory_path, 'remote_list.txt')
    if not options.no_refresh:
        print "Fetching remote listing...",
        with open(listfile_name, 'w') as listfile:
            #listfile.write(gsutil('ls', 'gs://'+bucketname))
            p = subprocess.Popen((gsutil_path, 'ls', 'gs://'+bucketname), stdout=subprocess.PIPE)
            for line in p.stdout:
                listfile.write(line)
            assert p.wait() == 0
        print "done."

    conn = sqlite3.connect(options.inventory_db)
    cursor = conn.cursor()
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_path ON files(path);')

    with open(listfile_name, 'r') as listfile:
        i = 0
        for line in listfile:
            path = line.strip()[len("gs://ge-mars/"):]
            cursor.execute('UPDATE files SET remote_exists = 1 WHERE path == ?', (path,))
            i += 1
            if i % DB_TRANSACTION_SIZE == 0:    
                conn.commit()
                print "%d updates" % i
        else:
            conn.commit()
    

if __name__ == "__main__":
    #parser = optparse.OptionParser()
#    parser.add_option('--dir', dest="source_dir")
#    parser.add_option('--bucket', dest="dest_bucket")
#    parser.add_option('--gsutil-path', dest='gsutil_path')
#    parser.add_option('--local-inventory', action='store_true', dest='local_inventory')

    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['local-inventory', 'remote-inventory', 'sync-serial', 'sync-parallel'])
    parser.add_argument('--dir', dest='source_dir', metavar='rood source directory', default=settings.OUTPUT_PATH_BASE)
    parser.add_argument('--bucket', dest='dest_bucket', default=settings.TARGET_GCS_BUCKET)
    parser.add_argument('--gsutil-path', dest='gsutil_path')
    parser.add_argument('--no-refresh', action='store_true', default=False, help="For remote inventory, don't refresh the GCS listing, just do the db updates.")
    parser.add_argument('-p', type=int, dest='num_processes', help='Number of subprocesses to spawn for sync-parallel', default=8)
    parser.add_argument('--max_queue_size', type=int, help="Size of the task queue", default=200)
    parser.add_argument('--db_chunk_size', type=int, help="Number of records to pull from the database per read", default=200)

    options = parser.parse_args()
    print "%s --> %s" % (options.source_dir, options.dest_bucket)

    # derived options
    options.inventory_path = '.'+options.dest_bucket
    options.inventory_db = os.path.join(options.inventory_path, 'inventory.db')

    connection = sqlite3.Connection(options.inventory_db)

    global gsutil_path
    if options.gsutil_path:
        gsutil_path = options.gsutil_path
    else:
        gsutil_path = search_for_gsutil()

    if options.command == 'local-inventory':
        local_inventory(options.source_dir, options.dest_bucket)
    elif options.command == 'remote-inventory':
        remote_inventory(options.source_dir, options.dest_bucket, options)
    elif options.command == 'sync-serial':
        sync_recursive(options.source_dir, options.dest_bucket)
    elif options.command == 'sync-parallel':
        sync_parallel(names_from_db(options.inventory_db), options)
    else:
        assert False
