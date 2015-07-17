"""
This contains some utility methods to share between the server and the client, or by the server components themself
"""
import contextlib
import fcntl
import os
import re
<<<<<<< Updated upstream
import subprocess
=======
import shutil
import tarfile
import tempfile
import time
>>>>>>> Stashed changes

def checkOutLFN(lfn, username):
    if lfn.startswith('/store/user/'):
        if lfn.split('/')[3] != username:
            return False
    elif lfn.startswith('/store/group/') or lfn.startswith('/store/local/'):
        if lfn.split('/')[3] == '':
            return False
    else:
        return False
    return True


#setDashboardLogs function is shared between the postjob and the job wrapper. Sharing it here
def setDashboardLogs(params, webdir, jobid, retry):
    log_files = [("job_out", "txt"), ("postjob", "txt")]
    for i, log_file in enumerate(log_files):
        log_file_basename, log_file_extension = log_file
        log_file_name = "%s/%s.%d.%d.%s" % (webdir, \
                                            log_file_basename, jobid, \
                                            retry, \
                                            log_file_extension)
        log_files[i] = log_file_name
    params['StatusLogFile'] = ",".join(log_files)

def getWebdirForDb(reqname, storage_rules):
    path = os.path.expanduser("~/%s" % reqname)
    sinfo = storage_rules.split(",")
    storage_re = re.compile(sinfo[0])
    val = storage_re.sub(sinfo[1], path)

    return val

def cmd_exist(cmd):
    """
    Check if linux command exist
    """
    try:
        null = open("/dev/null", "w")
        subprocess.Popen("/usr/bin/which %s" % cmd, stdout=null, stderr=null)
        null.close()
        return True
    except OSError:
        return False

@contextlib.contextmanager
def getLockedFile(name, mode, retries=5, sleep=0.5):
    try:
        fd = open(name, mode)
        fcntl.flock(fd, fcntl.LOCK_EX|fcntl.LOCK_NB)
    except:
        if retries == 0:
            raise Exception('unable to access locked file {0}'.format(name))
        time.sleep(sleep)
        with getLockedFile(name, mode, retries-1) as fd:
            yield fd

    try:
        yield fd

    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()

@contextlib.contextmanager
def getLockedAppendableTarFile(name, mode, retries=5, sleep=0.5):
    try:
        fd = open(name, 'a') # use open() because tarfile.open() objects do not have fileno() (needed for flock)
        fcntl.flock(fd, fcntl.LOCK_EX|fcntl.LOCK_NB)
    except:
        if retries == 0:
            raise Exception('unable to access locked file {0}'.format(name))
        time.sleep(sleep)
        with getLockedAppendableTarFile(name, mode, retries-1) as tfd:
            yield tfd

    try:
        tempDir = tempfile.mkdtemp()
        tfd = tarfile.open(name, mode.replace('w', 'r'))
        tfd.extractall(tempDir)
        tfd.close()
        tfd = tarfile.open(name, mode)

        yield tfd

    finally:
        tfd.add(tempDir, arcname='')
        tfd.close()
        shutil.rmtree(tempDir)
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()

