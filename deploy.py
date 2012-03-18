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
import tempfile

logger = multiprocessing.log_to_stderr()
#logger.setLevel(multiprocessing.SUBDEBUG)
logger.setLevel(logging.INFO)

DB_TRANSACTION_SIZE = 100000 # for bulk commits and updates to the inventory database
dblock = multiprocessing.Lock() # Using a global database lock because sqlite locks agressively and sometimes takes some time to unlock.

def patient(func):
    delay = 1 # time to wait before retrying after encountering a lock
    def wrapped(*args, **kwargs):
        i = 0
        while True:
            try:
                return func(*args, **kwargs)
            except OperationalError as e:
                if 'database is locked' in e.message:
                    logger.error("Database is locked.  Retrying. (%d)" % i)
                    time.sleep(delay)
                    i += 1
                    continue
                else:
                    raise
    return wrapped

class PatientCursor( sqlite3.Cursor ):
    @patient
    def execute(self, *args, **kwargs):
        super(PatientCursor, self).execute(*args, **kwargs)

class PatientConnection( sqlite3.Connection ):
    @patient
    def commit(self, *args, **kwargs):
        super(PatientConnection, self).commit(*args, **kwargs)

sqlite3.Cursor = PatientCursor # Monkey Patch
sqlite3.Connection = PatientConnection # Monkey Patch
        

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
    #gsutil('cp', path, "gs://"+bucket+"/"+relpath)
    logger.debug("UPLOADED " +path)

def transfer_dir_relative(path, bucket, root_path):
    """  
    Copy a given directory to a GCS bucket such that the object keys are all prefixed by 
    each file's path relative to root_path.  gsutil can't do this on it's own so we need to 
    create a temporary subtree on the filesystem representing the relative path and then
    recursively transfer that subtree.
    """
    tmpdir = tempfile.mkdtemp()
    relpath = os.path.relpath(path, root_path)
    logger.info("Transferring: %s" % relpath)

    if relpath != '.':
        os.makedirs(os.path.join(tmpdir, relpath))

    links = []
    for file in os.listdir(path):
        if not os.path.isdir(os.path.join(path, file)):
            os.symlink( os.path.join(path, file), os.path.join(tmpdir, relpath, file) )
            links.append( os.path.join(tmpdir, relpath, file) )

    gsutil('-m', 'cp', '-R', os.path.join(tmpdir, '*'), 'gs://'+bucket)

    # Clean up
    for link in links:
        os.unlink(link)
    if relpath != '.':
        os.removedirs(os.path.join(tmpdir, relpath)) # remove empty parents recursively

def transfer_chunk(paths, bucket, root_path):
    """  
    Copy a given sequence of filenames to a GCS bucket such that the object keys are all prefixed by 
    each file's path relative to root_path.  gsutil can't do this on it's own so we need to 
    create a temporary subtree on the filesystem representing the relative path and then
    recursively transfer that subtree.
    """
    tmpdir = tempfile.mkdtemp()
    relpaths = ( os.path.relpath(path, root_path) for path in paths ) if root_path in paths[0] else paths # assuming they're either all absolute or all relative
    logger.info("Transferring: %s and %d more..." % (relpaths[0], len(paths)-1) )

    for relpath in relpaths:
        try:
            os.makedirs(os.path.join(tmpdir, os.path.dirname(relpath) ))
        except OSError: pass

    links = []
    for relpath in relpaths:
        if not os.path.isdir(os.path.join(root_path, relpath)):
            symlink_target = os.path.join(tmpdir, relpath)
            if os.path.exists(symlink_target): os.unlink(symlink_target) # some failure modes have resulted in the link already existing... duplicate records? reissued tmp dirs?
            os.symlink( os.path.join(root_path, relpath), symlink_target )
            links.append( symlink_target )

    gsutil('-m', 'cp', '-R', os.path.join(tmpdir, '*'), 'gs://'+bucket)

    # Clean up
    subprocess.check_call( ('rm', '-rf', tmpdir) )

def sync_recursive(source_dir, dest_bucket):
    """ Sync files from source path to dest_bucket on GCS."""
    gsutil( "setdefacl", "public-read", "gs://"+dest_bucket) # make uploads world-readable
    print "%s --> gs://%s" % (source_dir, dest_bucket)
    for (dirpath, dirnames, filenames) in os.walk(source_dir):
        print len(filenames)
        for filename in filenames:
            sync_file(os.path.join(dirpath, filename), dest_bucket, source_dir)

def increment_string(s):
    return s[:-1] + chr(ord(s[-1])+1)

def set_path_status(filepaths, connection, status_value=1):
    """
    Update a path's status in the inventory db.
      NULL: unprocessed
      -1: enqueued for upload
      +1: upload complete 
    """
    global dblock

    # enforce that filepaths is a sequence
    if isinstance(filepaths, basestring):
        filepaths = (filepaths, )

    logging.info("Set status on %s and %d more" % (filepaths[0], len(filepaths)-1) )

    # ensure we're dealing with the relative path, which is what's stored in the inventory db
    # assuming that if one path is absolute, they all are
    if options.source_dir in filepaths[0]:
        filepaths = ( os.path.relpath(filepath, options.source_dir) for filepath in filepaths )

    if options.by_subdir:
        assert len(filepaths) == 1
        abspath = os.path.join(options.source_dir, filepaths[0])
        paths = []
        for filename in os.listdir(abspath):
            if not os.path.isdir(os.path.join(abspath, filename)):
                paths.append( os.path.join( filepath, filename) if filepath != '.' else filename)
    else:
        paths = filepaths # assuming here that filepath is a sequence of paths to mark

    if dblock: dblock.acquire()
    cursor = connection.cursor()
    for path in paths:
        logger.debug("UPDATING: %s" % path)
        cursor.execute('UPDATE files set remote_exists = ? where path = ?', (status_value, path))
        assert cursor.rowcount == 1
    logger.debug("UPDATED STATUS to %d: count %d" % (status_value, len(paths)) )
    connection.commit()
    time.sleep(0.2) # the db seems to need some time to releae its lock after a commit
    logger.debug("COMMITTED")
    cursor.close()
    if dblock: dblock.release()

def worker(task_q, result_q, finished, dbname):
    while True:
        if finished.is_set():
            break
        try:
            path, dest_bucket, start_path = task_q.get(False, 0.1) # retry after a timeout
            logger.debug("unqueued (%s, %s, %s)" % (path, dest_bucket, start_path) )
        except Queue.Empty:
            continue
        if options.by_subdir:
            transfer_dir_relative(path, dest_bucket, start_path)
        elif options.by_chunk:
            transfer_chunk(path, dest_bucket, start_path)
        else:
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
        set_path_status(path, connection, status_value=1)
        q.task_done()


def sync_parallel(source_path_iterator, options):
    global connection
    gsutil( "setdefacl", "public-read", "gs://"+options.dest_bucket) # make uploads world-readable
    response = None
    while response not in ('y','n'):
        response = raw_input("Do you want to run a remote inventory first?  You probably do, but it will take a while. (y/n)")
    if response =='y':
        remote_inventory(options.source_dir, options.dest_bucket)

    task_q = multiprocessing.JoinableQueue(options.max_queue_size)
    result_q = multiprocessing.JoinableQueue(options.max_queue_size)
    finished = multiprocessing.Event()
    global dblock # using a global database lock, because sqlite locking is agressive and a bit sticky.
    subprocesses = [ multiprocessing.Process(target=worker, args=(task_q, result_q, finished, options.inventory_db)) for i in range(options.num_processes) ]
    reporter = multiprocessing.Process(target=db_reporter, args=(result_q, finished, options.inventory_db) )
    subprocesses.append(reporter)
    for p in subprocesses:
        p.start()
    for path in source_path_iterator:
        set_path_status(path, connection, status_value=-1)
        task_q.put( (path, options.dest_bucket, options.source_dir) )
    task_q.join()
    result_q.join()
    finished.set()
    logger.info("Finished.  Waiting for subprocesses to join")
    for p in subprocesses:
        p.join()

def get_subdirs(root_dir):
    """ yield all the subdirectories of a path that have files in them"""
    for (dirpath, dirnames, filenames) in os.walk(root_dir):
        if len(filenames) > 0:
            yield dirpath

def names_from_db(dbname, which="untransfered", dblock=None):
    global connection
    qry = "SELECT id,path FROM files"
    if which != "all":
        qry += " WHERE remote_exists IS NULL"
    qry += " LIMIT %d" % options.db_chunk_size
    logger.debug( qry )
    while True:
        if dblock: dblock.acquire()
        cursor = connection.cursor()
        cursor.execute(qry)
        records = cursor.fetchall()
        cursor.close()
        if dblock: dblock.release()
        if len(records) == 0: break
        for record in records:
            yield record[1]

def get_chunks(dbname, dblock=None):
    path_generator =  names_from_db(dbname,  dblock=dblock)
    while True:
        paths = []
        for i in range( options.db_chunk_size):
            try:
                paths.append( path_generator.next() )
            except StopIteration: break
        if len(paths) < 1: break
        yield tuple(paths)

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
    if os.path.exists(options.inventory_db):
        response = None   
        while response not in ('y','n'):
            response = raw_input("Database exists at %s.  Clobber it? (y/n)" % options.inventory_db)
        if response == 'n':
            return False
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
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_remote_exists ON files(remote_exists);')
    print "Done."

def remote_inventory(source_dir, bucketname, options):
    inventory_path = options.inventory_path 
    assert os.path.exists(options.inventory_db)

    listfile_name = os.path.join(inventory_path, 'remote_list.txt')
    if not options.no_refresh:
        print "Fetching remote listing...",
        sys.stdout.flush()
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
    parser.add_argument('--by-subdir', action='store_true', default=False, help="For parallel sync, optimize by passing entire subdirectories to gsutil.  This mode doesn't sync.  It just pushes.")
    parser.add_argument('--by-chunk', action='store_true', default=False, help="For parallel sync, optimize by passing entire chunks to gsutil (of size db_chunk_size).")
    parser.add_argument('-p', type=int, dest='num_processes', help='Number of subprocesses to spawn for sync-parallel', default=8)
    parser.add_argument('--max_queue_size', type=int, help="Size of the task queue", default=20)
    parser.add_argument('--db_chunk_size', type=int, help="Number of records to pull from the database per read", default=200)

    options = parser.parse_args()
    print "%s --> %s" % (options.source_dir, options.dest_bucket)

    # derived options
    options.inventory_path = '.'+options.dest_bucket
    options.inventory_db = os.path.join(options.inventory_path, 'inventory.db')
    
    connection = sqlite3.Connection(options.inventory_db)

    # look for gsutil in some reasonable places if it's not provided at the command line
    global gsutil_path
    if options.gsutil_path:
        gsutil_path = options.gsutil_path
    else:
        gsutil_path = search_for_gsutil()

    # Clear any pending-transfer status that may have been set if a previous run was aborted.
    print "Resetting enqueued transfers...",
    cursor = connection.cursor()
    cursor.execute("UPDATE files SET remote_exists = NULL WHERE remote_exists == -1")
    connection.commit()
    cursor.close; del cursor
    print "Done."

    if options.command == 'local-inventory':
        local_inventory(options.source_dir, options.dest_bucket)
    elif options.command == 'remote-inventory':
        remote_inventory(options.source_dir, options.dest_bucket, options)
    elif options.command == 'sync-serial':
        if options.by_subdir:
            raise Exception("--by-subdir is only implemented in parallel sync mode.")
        sync_recursive(options.source_dir, options.dest_bucket)
    elif options.command == 'sync-parallel':
        assert not options.by_subdir and options.by_chunk
        if options.by_subdir:
            sync_parallel(get_subdirs(options.source_dir), options)
        elif options.by_chunk:
            sync_parallel(get_chunks(options.source_dir, dblock=dblock), options)
        else:
            sync_parallel(names_from_db(options.inventory_db, dblock=dblock), options)
    else:
        assert False # argparse shouldn't let us get to this condition
