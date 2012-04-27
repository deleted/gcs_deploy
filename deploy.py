#!/usr/bin/env python
import settings
import httplib
import hashlib
import os
import sys
import time
import shlex
import subprocess
import argparse
import sqlite3
import multiprocessing, logging
import Queue
import tempfile
import itertools

#logger = multiprocessing.log_to_stderr()
#logger.setLevel(multiprocessing.SUBDEBUG)
logger = logging.getLogger()
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
                    logger.warning("Database is locked.  Retrying. (%d)" % i)
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

def touch(fname, times=None, time=None):
    if time and not times:
        # set atime and mtime both equal to time
        times = (time, time)
    with file(fname, 'a'):
        os.utime(fname, times) # times is None for current time, or a tuple of timestamps: (atime, mtime)

def list_modified_files(root_path, reference_file):
    """
    Search the filesystem, starting at root_path.
    Return a list of all files in the subtree with modification date
    greater than that of a reference file
    """
    logger.info( "Searching for files modified since: %s" % time.ctime(os.path.getmtime( reference_file )) )
    args = (
        'find',
        root_path,
        '-newer', reference_file
    )
    logging.debug(' '.join(args) )
    args = shlex.split( ' '.join( args ) )
    p = subprocess.Popen(args, stdout=subprocess.PIPE)
    for line in p.stdout.readlines():
        path = line.strip()
        if not os.path.isdir(path):
            relpath = os.path.relpath(path, root_path)
            logger.info("Found modified file: %s" % relpath)
            yield relpath

def show_modified(options):
    sync_timestamp_file = options.sync_timestamp_file
    if not os.path.exists( sync_timestamp_file ):
        print "%s does not exist.  Aborting." % sync_timestamp_file
        sys.exit(1)

    for file in list_modified_files(options.source_dir, sync_timestamp_file):
        print file
        sys.stdout.flush()

def prep_db_for_update( connection, path_iterator ):
    """
    path_iterator here is a list of files that have been modified.
    Prior to initiating the transfer, mark all these files as untransferred in the database,
    creating records if they don't already exist.
    """
    logger.info("Prepping DB for date-based update.")
    cursor = connection.cursor()
    old = new = 0
    for path in path_iterator:
        cursor.execute("SELECT count(id) FROM files WHERE path = ?", (path,) )
        if int(cursor.fetchone()[0]) > 0:
            cursor.execute( 'UPDATE files SET transferred = 0 WHERE path = ?', (path,) )
            old += 1
        else:
            cursor.execute('INSERT INTO files (path, transferred) VALUES (?,?);',  (path, 0) )
            new += 1
    logger.info("Will update %d existing files and add %d new files." % (old, new) )
    connection.commit()
    cursor.close()
    logger.debug("Committed DB prep.")

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
        cursor.execute('UPDATE files set transferred = ? where path = ?', (status_value, path))
        logger.debug("Query rowcount: "+str(cursor.rowcount))
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
#    while response not in ('y','n'):
#        response = raw_input("Do you want to run a remote inventory first?  You probably do, but it will take a while. (y/n)")
#    if response =='y':
#        remote_inventory(options.source_dir, options.dest_bucket)

    task_q = multiprocessing.JoinableQueue(options.max_queue_size)
    result_q = multiprocessing.JoinableQueue(options.max_queue_size)
    finished = multiprocessing.Event()
    global dblock # using a global database lock, because sqlite locking is agressive and a bit sticky.
    subprocesses = [ multiprocessing.Process(target=worker, args=(task_q, result_q, finished, options.inventory_db)) for i in range(options.num_processes) ]
    reporter = multiprocessing.Process(target=db_reporter, args=(result_q, finished, options.inventory_db) )
    subprocesses.append(reporter)
    for p in subprocesses:
        p.start()
    for paths in source_path_iterator:
        set_path_status(paths, connection, status_value=-1)
        task_q.put( (paths, options.dest_bucket, options.source_dir) )
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
        qry += " WHERE transferred IS NULL"
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

def get_chunks(path_generator):
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
            remote_exists INTEGER,
            transferred INTEGER
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
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transferred ON files(transferred);')
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
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['local-inventory', 'remote-inventory', 'sync-serial', 'sync-parallel', 'show-modified'])
    parser.add_argument('--dir', dest='source_dir', metavar='rood source directory', default=settings.OUTPUT_PATH_BASE)
    parser.add_argument('--bucket', dest='dest_bucket', default=settings.TARGET_GCS_BUCKET)
    parser.add_argument('--gsutil-path', dest='gsutil_path')
    parser.add_argument('--no-refresh', action='store_true', default=False, help="For remote inventory, don't refresh the GCS listing, just do the db updates.")
    parser.add_argument('--by-subdir', action='store_true', default=False, help="For parallel sync, optimize by passing entire subdirectories to gsutil.  This mode doesn't sync.  It just pushes.")
    parser.add_argument('--by-chunk', action='store_true', default=True, help="For parallel sync, optimize by passing entire chunks to gsutil (of size db_chunk_size).")
    parser.add_argument('-p', type=int, dest='num_processes', help='Number of subprocesses to spawn for sync-parallel', default=8)
    parser.add_argument('--max_queue_size', type=int, help="Size of the task queue", default=20)
    parser.add_argument('--db_chunk_size', type=int, help="Number of records to pull from the database per read", default=200)
    parser.add_argument('--debug', action='store_true', default=False, help="Turn on debug logging.")

    options = parser.parse_args()
    print "%s --> %s" % (options.source_dir, options.dest_bucket)

    # derived options
    options.inventory_path = '.'+options.dest_bucket
    options.inventory_db = os.path.join(options.inventory_path, 'inventory.db')
    options.sync_timestamp_file = os.path.join( options.inventory_path, 'last_sync' ) # this file exists solely to mark the time the last sync completed.
    
    if options.debug:
        logger.setLevel(logging.DEBUG)
        multiprocessing.log_to_stderr().setLevel(multiprocessing.SUBDEBUG)


    connection = sqlite3.Connection(options.inventory_db)

    # look for gsutil in some reasonable places if it's not provided at the command line
    global gsutil_path
    if options.gsutil_path:
        gsutil_path = options.gsutil_path
    else:
        gsutil_path = search_for_gsutil()

    start_time = time.time()

    # Clear any pending-transfer status that may have been set if a previous run was aborted.
    print "Resetting enqueued transfers...",
    cursor = connection.cursor()
    cursor.execute("UPDATE files SET remote_exists = NULL WHERE remote_exists == -1")
    connection.commit()
    cursor.close()
    del cursor
    print "Done."

    logging.debug("Start mode switching.")

    if options.command == 'local-inventory':
        local_inventory(options.source_dir, options.dest_bucket)
    elif options.command == 'show-modified':
        show_modified(options)
    elif options.command == 'remote-inventory':
        remote_inventory(options.source_dir, options.dest_bucket, options)
    elif options.command == 'sync-serial':
        if options.by_subdir:
            raise Exception("--by-subdir is only implemented in parallel sync mode.")
        sync_recursive(options.source_dir, options.dest_bucket)
        touch( options.sync_timestamp_file, time=start_time) # update last sync timestamp
    elif options.command == 'sync-parallel':
        assert not options.by_subdir and options.by_chunk
        if options.by_subdir:
            path_generator = get_subdirs(options.source_dir, time=start_time)
        elif options.by_chunk:
            logging.debug("By chunk.")
            if os.path.exists( options.sync_timestamp_file ):
                # push everyting modified since the last sync
                logging.debug("Using date-modified.")
                modified1, modified2 = itertools.tee(  list_modified_files(options.source_dir, options.sync_timestamp_file), 2)
                prep_db_for_update( connection, modified1 ) # make sure all the db records exist
                path_generator = get_chunks ( modified2 )
            else:
                logging.debug("Usinge names from db.")
                path_generator = get_chunks( names_from_db(options.inventory_db, dblock=dblock) )
        else:
            path_generator = names_from_db(options.inventory_db, dblock=dblock)
        sync_parallel(path_generator, options)
        touch( options.sync_timestamp_file ) # update last sync timestamp
    else:
        assert False # argparse shouldn't let us get to this condition
