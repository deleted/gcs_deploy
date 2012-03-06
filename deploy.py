#!/usr/bin/env python
import settings
import httplib
import hashlib
import os
import sys
import subprocess
import optparse
import sqlite3

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

def sync_file(path, bucket, start_path):
    print path + "...",
    sys.stdout.flush()
    relpath = os.path.relpath(path, start_path)
    url = "http://commondatastorage.googleapis.com/%s/%s" % (bucket, relpath)
    fail = False
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
            print "MD5 mismatch... %s :: %s" % (local_hash, remote_hash)
    print "UPLOADING...",
    gsutil('cp', path, "gs://"+bucket+"/"+relpath)
    print "DONE!"

def sync_recursive(source_path, dest_bucket):
    """ Sync files from source path to dest_bucket on GCS."""
    gsutil( "setdefacl", "public-read", "gs://"+dest_bucket) # make uploads world-readable
    print "%s --> gs://%s" % (source_path, dest_bucket)
    for (dirpath, dirnames, filenames) in os.walk(source_path):
        print len(filenames)
        for filename in filenames:
            sync_file(os.path.join(dirpath, filename), dest_bucket, source_path)

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
    inventory_path = "."+bucketname
    ensure_exists(inventory_path)
    conn = sqlite3.connect(os.path.join(inventory_path, "inventory.db"))
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS files;")
    cursor.execute("""
    CREATE TABLE files (
        id INTEGER PRIMARY KEY,
        path TEXT NOT NULL,
        local_hash TEXT,
        remote_hash TEXT,
        remote_exists INTEGER
    );
    """)

    count = 0
    for (dirpath, dirnames, filenames) in os.walk(source_dir):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            filehash = md5_digest(filepath)
            cursor.execute('INSERT INTO files (path, local_hash) VALUES (?, ?);',  (filepath, filehash))
            count += 1
            if count % DB_TRANSACTION_SIZE == 0:
                conn.commit()
        print "%s: %d inserts." % (dirpath, count)
    else:
        conn.commit()
    print "Creating index...",
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_path ON files(path);')
    print "Done."

def remote_inventory(source_dir, bucketname):
    inventory_path = "."+bucketname
    assert os.path.exists(os.path.join(inventory_path, 'inventory.db'))

    print "Fetching remote listing...",
    listfile_name = os.path.join(inventory_path, 'remote_list.txt')
    with open(listfile_name, 'w') as listfile:
        listfile.write(gsutil('ls', 'gs://"+bucketname'))
    print "done."

    conn = sqlite3.connect(os.path.join(inventory_path, "inventory.db"))
    cursor = conn.cursor()
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_path ON files(path);')

    with open(listfile_name, 'r') as listfile:
        i = 0
        for line in listfile:
            path = line.strip()[len("gs://ge-mars/")-1:]
            cursor.execute('UPDATE files SET remote_exists = 1 WHERE path == ?', path)
            i += 1
            if i % DB_TRANSACTION_SIZE == 0:    
                conn.commit()
        else:
            conn.commit()
    

if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option('--dir', dest="source_dir")
    parser.add_option('--bucket', dest="dest_bucket")
    parser.add_option('--gsutil-path', dest='gsutil_path')
    parser.add_option('--local-inventory', action='store_true', dest='local_inventory')

    parser.set_defaults(source_dir=settings.OUTPUT_PATH_BASE, dest_bucket=settings.TARGET_GCS_BUCKET, gsutil_path=None)
    options, args = parser.parse_args()


    if options.gsutil_path:
        gsutil_path = options.gsutil_path
    else:
        gsutil_path = search_for_gsutil()

    if options.local_inventory:
        local_inventory(options.source_dir, options.dest_bucket)
    else:
        sync_recursive(options.source_dir, options.dest_bucket)
