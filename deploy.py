#!/usr/bin/env python2.6
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
import multiprocessing, logging, logging.handlers
from ctypes import c_int
import Queue
import tempfile
import itertools

#logger = multiprocessing.log_to_stderr()
#logger.setLevel(multiprocessing.SUBDEBUG)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

hdlr = logging.handlers.RotatingFileHandler('deploy.log', backupCount=1, maxBytes=0)
hdlr.doRollover()
formatter = logging.Formatter('%(asctime)s %(processName)s:%(lineno)d %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)

DB_TRANSACTION_SIZE = 100000 # for bulk commits and updates to the inventory database
SQLITE_TIMEOUT = 20.0 # seconds

dblock = multiprocessing.Lock() # Using a global database lock because sqlite locks agressively and sometimes takes some time to unlock.

# Logging DB lock for debugging
_acquire = dblock.acquire
_release = dblock.release
def acquire(*args, **kwargs):
    time.sleep(0.2)
    _acquire(*args, **kwargs)
    logger.debug("Lock acquired.")
def release( *args, **kwargs):
    time.sleep(0.2)
    _release( *args, **kwargs)
    logger.debug("Lock released.")
dblock.acquire = acquire
dblock.release = release
    

def patient(func):
    delay = 1 # time to wait before retrying after encountering a lock
    def wrapped(*args, **kwargs):
        i = 0
        while True:
            try:
                return func(*args, **kwargs)
            except sqlite3.OperationalError as e:
                if 'database is locked' in e.message:
                    logger.warning("Database is locked.  Retrying. (%d)" % i)
                    time.sleep(delay)
                    i += 1
                    continue
                else:
                    raise
    return wrapped


open_connections = multiprocessing.Value(c_int, 0, lock=True)
open_cursors = multiprocessing.Value(c_int, 0, lock=True)
class PatientCursor( sqlite3.Cursor ):
    def __init__(self, *args, **kwargs):
        open_cursors.value += 1
        logger.debug("New cursor (%d)" % open_cursors.value)
        return super(PatientCursor, self).__init__(*args, **kwargs)

    def close(self, *args, **kwargs):
        open_cursors.value -= 1
        logger.debug("Close cursor (%d)" % open_cursors.value)
        return super(PatientCursor, self).close(*args, **kwargs)

    @patient
    def execute(self, *args, **kwargs):
        logger.debug("PatientCursor.execute(): " + str(args))
        super(PatientCursor, self).execute(*args, **kwargs)

class PatientConnection( sqlite3.Connection ):
    def __init__(self, *args, **kwargs):
        open_connections.value += 1
        logger.debug("New connection (%d)" % open_connections.value)
        return super(PatientConnection, self).__init__(*args, **kwargs)

    def close(self, *args, **kwargs):
        open_connections.value -= 1
        logger.debug("Close connection (%d)" % open_connections.value)
        return super(PatientConnection, self).close(*args, **kwargs)

    def cursor(self, *args, **kwargs):
        return super(PatientConnection, self).cursor(PatientCursor, *args, **kwargs)

    @patient
    def commit(self, *args, **kwargs):
        logger.debug("PatientConnection.commit().")
        super(PatientConnection, self).commit(*args, **kwargs)
sqlite3.Connection = PatientConnection # Monkey Patch

def gsutil(*args):
    args = (gsutil_path,) + args
    print " ".join(args)
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = "\n".join( p.communicate() )
    if p.returncode == 0:
        return
    else:
        # emulate the behavior of Python 2.7's subprocess.check_output()
        e = subprocess.CalledProcessError(p.returncode, " ".join(args))
        e.output = output
        raise e
    #return subprocess.check_output(args)

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
    greater than that of a reference file.'-rf', tmpdir
    Cache the find results to a file so that it can be resumed next time.
    """
    modified_files_log = os.path.join(options.inventory_path, 'modified-files')
    if options.recycle and os.path.exists(modified_files_log):
        logger.info("skipping FIND and reusing %s" % modified_files_log)
        with open(modified_files_log, 'r') as infile:
            for line in infile:
                yield line
        return

    logger.info( "Searching for files modified since: %s" % time.ctime(os.path.getmtime( reference_file )) )
    args = (
        'find',
        root_path,
        '-newer', reference_file
    )
    logging.debug(' '.join(args) )
    args = shlex.split( ' '.join( args ) )
    p = subprocess.Popen(args, stdout=subprocess.PIPE)
    i = 0
    with open(modified_files_log, 'w') as logfile:
        for line in p.stdout.readlines():
            path = line.strip()
            if not os.path.isdir(path):
                    i += 1
                    relpath = os.path.relpath(path, root_path)
                    logger.info("Found modified file: %s" % relpath)
                    logfile.write(relpath + "\n")
                    yield relpath
        else:
            logger.info("FIND iterator is exhausted (%d paths yielded)." % i)

def show_modified(options):
    sync_timestamp_file = options.sync_timestamp_file
    if not os.path.exists( sync_timestamp_file ):
        print "%s does not exist.  Aborting." % sync_timestamp_file
        sys.exit(1)

    for file in list_modified_files(options.source_dir, sync_timestamp_file):
        print file
        sys.stdout.flush()

def prep_db_for_update( path_iterator ):
    """
    path_iterator here is a list of files that have been modified.
    Prior to initiating the transfer, mark all these files as untransferred in the database,
    creating records if they don't already exist.
    """
    logger.info("Prepping DB for date-based update.")
    global dblock, options
    dblock.acquire()
    connection = sqlite3.connect(options.inventory_db, timeout=SQLITE_TIMEOUT, factory=PatientConnection)
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
    connection.close()
    dblock.release()

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

def transfer_chunk(paths, bucket, root_path):
    """  
    Copy a given sequence of filenames to a GCS bucket such that the object keys are all prefixed by 
    each file's path relative to root_path.  gsutil can't do this on it's own so we need to 
    create a temporary subtree on the filesystem representing the relative path tree and 
    then recursively transfer that subtree.
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
            logger.debug("Added symlink: " + symlink_target)

    args = ('-m', 'cp', '-R', os.path.join(tmpdir, '*'), 'gs://'+bucket)
    gsutil(*args)

    # Clean up
    logger.info("Deleting tmpdir: "+tmpdir)
    subprocess.check_call( ('rm', '-rf', tmpdir) )

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

    dblock.acquire()
    try:
        cursor = connection.cursor()
        for path in filepaths:
            logger.debug("UPDATING: %s" % path)
            #cursor.execute('UPDATE files set transferred = ? where path = ?', (status_value, path))
            cursor.execute('UPDATE files set transferred = ? where path = ?', (status_value, path))
            logger.debug("Query rowcount: "+str(cursor.rowcount))
            assert cursor.rowcount == 1
        logger.debug("UPDATED STATUS to %d: count %d" % (status_value, len(filepaths)) )
        connection.commit()
        time.sleep(0.2) # the db seems to need some time to releae its lock after a commit
        logger.debug("COMMITTED")
    finally:
        cursor.close()
        dblock.release()

def worker(task_q, result_q, finished, dbname):
    q_empty_logged = False
    while True:
        if finished.is_set():
            break
        try:
            job_number, path, dest_bucket, start_path = task_q.get(False, 0.1) # retry after a timeout
            #logger.debug("unqueued (%s, %s, %s)" % (path, dest_bucket, start_path) )
            logger.debug("unqueued job %d" % job_number)
            q_empty_logged = False
        except Queue.Empty:
            if (not q_empty_logged):
                logger.debug("Task queue empty.")
                q_empty_logged = True
            continue
        if options.by_chunk:
            transfer_chunk(path, dest_bucket, start_path)
        else:
            sync_file(path, dest_bucket, start_path, check_remote=False)
        logger.debug("finished job %d" % job_number)
        result_q.put(path)
        task_q.task_done()
        logger.debug("job %d results delivered" % job_number)
    
def db_reporter(q, finished, dbname):
    connection = sqlite3.connect(options.inventory_db, timeout=SQLITE_TIMEOUT, factory=PatientConnection)
    while True:
        if finished.is_set():
            break
        try:
            path = q.get(False, 0.1) # retry after a timeout
        except Queue.Empty:
            time.sleep(0.2)
            continue
        logging.debug("db_reporter marking got a result")
        set_path_status(path, connection, status_value=1)
        q.task_done()


def sync_parallel(source_path_iterator, options):
    logger.info("Beginning parallel sync.")
    connection = sqlite3.connect(options.inventory_db, timeout=SQLITE_TIMEOUT, factory=PatientConnection)
    gsutil( "setdefacl", "public-read", "gs://"+options.dest_bucket) # make uploads world-readable
    response = None
#    while response not in ('y','n'):
#        response = raw_input("Do you want to run a remote inventory first?  You probably do, but it will take a while. (y/n)")
#    if response =='y':
#        remote_inventory(options.source_dir, options.dest_bucket)

    task_q = multiprocessing.JoinableQueue(options.max_queue_size)
    result_q = multiprocessing.JoinableQueue(options.max_queue_size)
    global delete_queue
    delete_queue = multiprocessing.JoinableQueue()
    finished = multiprocessing.Event()
    logger.debug("Initializing subprocesses")
    subprocesses = [ multiprocessing.Process(target=worker, name="worker_"+str(i), args=(task_q, result_q, finished, options.inventory_db)) for i in range(options.num_processes) ]
    reporter = multiprocessing.Process(target=db_reporter, name="reporter", args=(result_q, finished, options.inventory_db) )
    subprocesses.append(reporter)
    subprocesses.append( multiprocessing.Process( target=delete_marker_worker, name="deleteMarker", args=(delete_queue, finished) ) )
    logger.debug("Starting subprocesses")
    for p in subprocesses:
        p.start()
    global delete_worker_started # used by mark_for_deletion()
    delete_worker_started = True

    job_number = 0
    path_count = 0
    for paths in source_path_iterator:
        job_number += 1
        path_count += len(paths)
        logger.debug("Setting up job #%d (%d paths)" % (job_number, len(paths)))
        set_path_status(paths, connection, status_value=-1)
        task_q.put( (job_number, paths, options.dest_bucket, options.source_dir) )
        logger.debug("Put job #%d into task_q" % job_number)
        logger.debug("Total paths dispatched: %d" % path_count)
    else:
        logger.debug("source_path_iterator exhausted")
        logger.debug("Total paths dispatched: %d" % path_count)
    logger.debug("Waiting for task_q to join.")
    task_q.join()
    logger.debug("Waiting for result_q to join.")
    result_q.join()
    logger.debug("Waiting for delete_queue to join.")
    delete_queue.join()
    logger.debug("Setting the finished event")
    finished.set()
    logger.info("Finished.  Waiting for subprocesses to join")
    for p in subprocesses:
        p.join()
    logger.info("All subprocesses joined.")

def _mark_for_deletion(connection, record_id, relpath):
    global dblock
    logger.info("Marking for deletion: %s" % relpath)
    dblock.acquire() 
    cursor = connection.cursor()
    cursor.execute("UPDATE files SET local_deleted = 1 where id = %d" % record_id)
    connection.commit()
    time.sleep(0.1)
    cursor.close()
    dblock.release() 

delete_worker_started = False
def mark_for_deletion(connection, record_id, relpath):
    global delete_worker_started, delete_queue
    if delete_worker_started:
        delete_queue.put((record_id, relpath)) # connection doesn't get used in this mode.
    else:
        _mark_for_deletion(connection, record_id, relpath)

def delete_marker_worker(delete_queue, finished):
    connection = sqlite3.connect(options.inventory_db, timeout=SQLITE_TIMEOUT, factory=PatientConnection)
    while not finished.is_set():
        try:
            record_id, relpath = delete_queue.get(False, 0.5)
            _mark_for_deletion(connection, record_id, relpath)
            delete_queue.task_done()
        except Queue.Empty:
            time.sleep(0.5)
            continue
    connection.close()

def names_from_db(dbname, which="untransfered", exclude_deleted=True):
    global dblock, options
    connection = sqlite3.connect(options.inventory_db, timeout=SQLITE_TIMEOUT, factory=PatientConnection)
    last_max_id = -9999
    while True:
        qry = "SELECT id,path FROM files"
        qry += " WHERE id > %d " % last_max_id
        if which == 'local_deleted':
            exclude_deleted = False
            qry += " AND local_deleted = 1"
        elif which == "untransfered":
            qry += " AND ( transferred IS NULL OR transferred != 1)"
        if exclude_deleted:
            qry += " AND local_deleted != 1"
        qry += " ORDER BY id "
        qry += " LIMIT %d" % options.db_chunk_size
        logger.debug( qry )

        dblock.acquire()
        cursor = connection.cursor()
        cursor.execute(qry)
        records = cursor.fetchall()
        connection.commit()
        time.sleep(0.1)
        cursor.close()
        dblock.release()
        logger.debug("names_from_db pulled %d paths from the database." % len(records))
        if len(records) == 0: 
            break
        for record in records:
            # Only return records that still exist on the local filesystem, unless we're in cleanup mode
            if (which == 'local_deleted') or os.path.exists( os.path.join(options.source_dir, record[1]) ):
                yield record[1]
            else:
                mark_for_deletion( connection, record[0], record[1] )
        else:
            last_max_id = int(record[0])
    connection.close()

def get_chunks(path_generator):
    """
    Group the output of path_generator into tuples of length options.db_chunk_size.
    """
    while True:
        paths = []
        for i in range( options.db_chunk_size):
            try:
                paths.append( path_generator.next() )
            except StopIteration:
                logger.debug("get_chunks encountered StopIteration")
                break
        if len(paths) < 1:
            break
        logger.debug("get_chunks yielding %d paths" % len(paths) )
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
    
def ensure_exists(dirname):
    if not os.path.exists(dirname):
        os.mkdir(dirname)

def local_inventory(source_dir, bucketname):
    """
    Walk the local filesystem starting at source_dir and create a database cataloging the files in that
    directory tree.
    """
    global dblock
    if os.path.exists(options.inventory_db):
        response = None   
        while response not in ('y','n'):
            response = raw_input("Database exists at %s.  Clobber it? (y/n)" % options.inventory_db)
        if response == 'n':
            return False
    inventory_path = options.inventory_path
    ensure_exists(inventory_path)
    conn = sqlite3.connect(options.inventory_db)
    dblock.acquire()
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
            transferred INTEGER,
            local_deleted INTEGER DEFAULT 0,
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
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_delete ON files(local_deleted);')
    print "Done."
    dblock.release()

def remote_inventory(source_dir, bucketname, options):
    """
    Fetch the complete list of files that exist in the GCS bucket.
    Scan that list and mark all the corresponding records in the iventory database as
    existent on the remote side.
    """
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

    global dblock
    dblock.acquire()
    conn = sqlite3.connect(options.inventory_db)
    cursor = conn.cursor()
    cursor.execute('UPDATE files SET remote_exists = NULL;')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_path ON files(path);')
    conn.commit()

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
    dblock.release()
    
###
# Clean up by deleting old files from GCS.
# Especially important for the live THEMIS images that generate sparse quad trees that are different every day.

def _db_delete_worker(delete_q, finished):
    global dblock, options
    logger.info("Delete worker is running")
    connection = sqlite3.connect(options.inventory_db, timeout=SQLITE_TIMEOUT, factory=PatientConnection)
    cursor = connection.cursor()
    while not finished.is_set():
        try:
            path = delete_q.get()
        except Queue.Empty:
            time.sleep(0.5)
            continue
        dblock.acquire()
        cursor.execute("DELETE FROM files WHERE path = '%s'" % path)
        if cursor.rowcount != 1:
            logger.warning( "Rowcount was %d after deleting %s" % (cursor.rowcount, path) )
        connection.commit()
        dblock.release()
        delete_q.task_done()
    logger.info("Delete worker is terminating")

def cleanup():
    global options
    paths = names_from_db(options.inventory_db, which='local_deleted')
    delete_q = multiprocessing.JoinableQueue()
    finished = multiprocessing.Event()
    delete_worker = multiprocessing.Process( target=_db_delete_worker, name="deleteWorker", args=(delete_q, finished) )
    delete_worker.start()
    for relpath in paths:
        logger.info("removing from GCS: "+relpath)
        try:
            gsutil('rm', 'gs://%s/%s' % (options.dest_bucket, relpath) )
        except subprocess.CalledProcessError, e:
            if not ("code=NoSuchKey" in e.output or "reason=Not Found" in e.output): # 404.  It's already beed deleted.
                raise e
        delete_q.put(relpath)
    print "Finished cleanup.  Shutting down."
    delete_q.join()
    finished.set()
    delete_worker.join()

# end clean up
###

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['local-inventory', 'remote-inventory', 'sync', 'sync-parallel', 'show-modified', 'cleanup'])

    parser.add_argument('--dir', dest='source_dir', metavar='rood source directory', default=settings.OUTPUT_PATH_BASE)
    parser.add_argument('--bucket', dest='dest_bucket', default=settings.TARGET_GCS_BUCKET)
    parser.add_argument('--gsutil-path', dest='gsutil_path')
    parser.add_argument('--by-chunk', action='store_true', default=True, help="For parallel sync, optimize by passing entire chunks to gsutil (of size db_chunk_size).")

    parser.add_argument('--no-refresh', action='store_true', default=False, help="For remote inventory, don't refresh the GCS listing, just do the db updates.")
    parser.add_argument('--skip-db', dest='use_db', action='store_false', default=True, help="On sync, don't look for untransferred files in the DB.")
    parser.add_argument('--skip-modtime', dest='use_modtime', action='store_false', default=True, help="On sync, don't look for files modified since the last sync.") 

    parser.add_argument('-p', type=int, dest='num_processes', help='Number of subprocesses to spawn for sync-parallel', default=8)
    parser.add_argument('--max_queue_size', type=int, help="Size of the task queue", default=20)
    parser.add_argument('--db_chunk_size', type=int, help="Number of records to pull from the database per read", default=200)
    parser.add_argument('--recycle', action='store_true', default=False, help="Draw the list of modified files from a logfile instead of scanning for them (speeds up debugging).")
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


    connection = sqlite3.connect(options.inventory_db, timeout=SQLITE_TIMEOUT, factory=PatientConnection)

    # look for gsutil in some reasonable places if it's not provided at the command line
    global gsutil_path
    if options.gsutil_path:
        gsutil_path = options.gsutil_path
    else:
        gsutil_path = search_for_gsutil()

    start_time = time.time()

    # Clear any pending-transfer status that may have been set if a previous run was aborted.
    print "Resetting enqueued transfers...",
    dblock.acquire() # dblock is global
    cursor = connection.cursor()
    cursor.execute("UPDATE files SET remote_exists = NULL WHERE remote_exists == -1")
    connection.commit()
    cursor.close()
    del cursor
    print "Done."
    dblock.release()

    logging.debug("Start mode switching.")

    if options.command == 'local-inventory':
        local_inventory(options.source_dir, options.dest_bucket)
    elif options.command == 'show-modified':
        show_modified(options)
    elif options.command == 'remote-inventory':
        remote_inventory(options.source_dir, options.dest_bucket, options)
    elif options.command == 'sync-parallel' or options.command == 'sync':
        path_sources = []
        if options.use_db:
            logging.debug("use_db: Getting untransferred files from the db.")
            path_sources.append( names_from_db(options.inventory_db) )
        if options.use_modtime:
            logging.debug("use_modtime: Checking for modified files.")
            modified1, modified2 = itertools.tee(  list_modified_files(options.source_dir, options.sync_timestamp_file), 2)
            prep_db_for_update( modified1 ) # make sure all the db records exist
            #path_sources.append( get_chunks ( modified2 ) )
            path_sources.append(  modified2 ) 

        logger.info("Chaining from %d path sources" % len(path_sources))
        path_generator = itertools.chain( *path_sources )

        if options.by_chunk:
            logger.info( "Chunking chained path sources." )
            path_generator = get_chunks( path_generator )
            
        sync_parallel(path_generator, options)

        if options.use_modtime: # only touch the timestamp file if we used modtime for the sync
            touch( options.sync_timestamp_file ) # update last sync timestamp

    elif options.command == 'cleanup':
        # Delete remote objects that have been deleted locally.
        cleanup()

    else:
        assert False # argparse shouldn't let us get to this condition
