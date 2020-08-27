"""
This module aims to contain the method specific to the REST interface.
These are extensions which are not directly contained in WMCore.REST module.
Collecting all here since aren't supposed to be many.
"""

from ServerUtilities import USER_SANDBOX_EXCLUSIONS, NEW_USER_SANDBOX_EXCLUSIONS
from ServerUtilities import FILE_SIZE_LIMIT, FILE_MEMORY_LIMIT

# WMCore dependecies here
from WMCore.REST.Validation import _validate_one
from WMCore.REST.Error import RESTError, InvalidParameter
from WMCore.Services.UserFileCache.UserFileCache import calculateChecksum
from WMCore.REST.Auth import get_user_info

# external dependecies here
import tarfile
import hashlib
import cStringIO
import urllib2
from os import fstat, walk, path, listdir

# 600MB is the default user quota limit - overwritten in RESTBaseAPI if quota_user_limit is set in the config
QUOTA_USER_LIMIT = 1024*1024*600
#these users have 10* basic user quota - overwritten in RESTBaseAPI if powerusers is set in the config
POWER_USERS_LIST = []

def http_error(msg, code=403):
    try:
        import requests
        err = requests.HTTPError(msg)
        err.response.status_code = code
        return err
    except ImportError:
        url = '' # required for urllib2 HTTPError but we should acquire it from elsewhere
        return urllib2.HTTPError(url, code, err, None, None)

###### authz_login_valid is currently duplicatint CRABInterface.RESTExtension . A better solution
###### should be found for authz_*
def authz_login_valid():
    user = get_user_info()
    if not user['login']:
        err = "You are not allowed to access this resources"
        raise http_error(err)

def authz_operator(username):
    """ Check if the the user who is trying to access this resource (i.e.: user['login'], the cert username) is the
        same as username. If not check if the user is a CRAB3 operator. {... 'operator': {'group': set(['crab3']) ... in request roles}
        If the user is not an operator and is trying to access a file owned by another user than raise
    """
    user = get_user_info()
    if user['login'] != username and\
       'crab3' not in user.get('roles', {}).get('operator', {}).get('group', set()):
        err = "You are not allowed to access this resource. You need to be a CRAB3 operator in sitedb to access other user's files"
        raise http_error(err)

def file_size(argfile):
    """Return the file or cStringIO.StringIO size

       :arg file|cStringIO.StringIO argfile: file object handler or cStringIO.StringIO
       :return: size in bytes"""
    if isinstance(argfile, file):
        return fstat(argfile.fileno()).st_size, True
    elif isinstance(argfile, cStringIO.OutputType):
        argfile.seek(0, 2)
        filesize = argfile.tell()
        argfile.seek(0)
        return filesize, False

def list_users(cachedir):
    #file are stored in directories like u/username
    for name in listdir(cachedir): #iterate over u ...
        if name == 'lost+found':  # skip root-owned file at top mount point
            continue
        if path.isdir(path.join(cachedir, name)):
            for username in listdir(path.join(cachedir, name)): #list all the users under u
                yield username

def list_files(quotapath):
    for _, _, filenames in walk(quotapath):
        for f in filenames:
            yield f

def get_size(quotapath):
    """Check the quotapath directory size; it doesn't include the 4096 bytes taken by each directory

    :arg str quotapath: the directory for which is needed to calculate the quota
    :return: bytes taken by the directory"""
    totalsize = 0
    for dirpath, _, filenames in walk(quotapath):
        for f in filenames:
            fp = path.join(dirpath, f)
            totalsize += path.getsize(fp)
    return totalsize

def quota_user_free(quotadir, infile):
    """Raise an exception if the input file overflow the user quota

    :arg str quotadir: the user path where the file will be written
    :arg file|cStringIO.StringIO infile: file object handler or cStringIO.StringIO
    :return: Nothing"""
    filesize, _ = file_size(infile.file)
    quota = get_size(quotadir)
    user = get_user_info()
    quotaLimit = QUOTA_USER_LIMIT*10 if user['login'] in POWER_USERS_LIST else QUOTA_USER_LIMIT
    if filesize + quota > quotaLimit:
         excquota = ValueError("User %s has reached quota of %dB: additional file of %dB cannot be uploaded." \
                               % (user['login'], quota, filesize))
         raise InvalidParameter("User quota limit reached; cannot upload the file", errobj=excquota, trace='')

def _check_file(argname, val):
    """Check that `argname` `val` is a file

       :arg str argname: name of the argument
       :arg file val: the file object
       :return: the val if the validation passes."""
    # checking that is a valid file or an input string
    # note: the input string is generated on client side just when the input file is empty
    filesize = 0
    if not hasattr(val, 'file') or not (isinstance(val.file, file) or isinstance(val.file, cStringIO.OutputType)):
        raise InvalidParameter("Incorrect inputfile parameter")
    else:
        filesize, realfile = file_size(val.file)
        if realfile:
            if filesize > FILE_SIZE_LIMIT:
                raise InvalidParameter("File size is %sB. This is bigger than the maximum allowed size of %sB." % (filesize, FILE_SIZE_LIMIT))
        elif filesize > FILE_MEMORY_LIMIT:
            raise InvalidParameter('File too large to be completely loaded into memory.')

    return val


def _check_tarfile(argname, val, hashkey, newchecksum):
    """Check that `argname` `val` is a tar file and that provided 'hashkey`
       matches with the hashkey calculated on the `val`.

       :arg str argname: name of the argument
       :arg file val: the file object
       :arg str hashkey: the sha256 hexdigest of the file, calculated over the tuple
                         (name, size, mtime, uname) of all the tarball members
       :return: the val if the validation passes."""
    # checking that is a valid file or an input string
    # note: the input string is generated on client side just when the input file is empty
    _check_file(argname, val)

    digest = None
    try:
        #This newchecksum param and the if/else branch is there for backward compatibility.
        #The parameter, older exclusion and checksum functions should be removed in the future.
        if newchecksum == 2:
            digest = calculateChecksum(val.file, exclude=NEW_USER_SANDBOX_EXCLUSIONS)
        elif newchecksum == 1:
            digest = calculateChecksum(val.file, exclude=USER_SANDBOX_EXCLUSIONS)
        else:
            tar = tarfile.open(fileobj=val.file, mode='r')
            lsl = [(x.name, int(x.size), int(x.mtime), x.uname) for x in tar.getmembers()]
            hasher = hashlib.sha256(str(lsl))
            digest = hasher.hexdigest()
    except tarfile.ReadError:
        raise InvalidParameter('File is not a .tgz file.')
    if not digest or hashkey != digest:
        raise ChecksumFailed("Checksums do not match")
    return val

class ChecksumFailed(RESTError):
    "Checksum calculation failed, file transfer problem."
    http_code = 400
    app_code = 302
    message = "Input file hashkey mismatch"

def validate_tarfile(argname, param, safe, hashkey, optional=False):
    """Validates that an argument is a file and matches the hashkey.

    Checks that an argument named `argname` exists in `param.kwargs`
    and it is a tar file which matches the provided hashkey. If
    successful the string is copied into `safe.kwargs` and the value
    is removed from `param.kwargs`.

    If `optional` is True, the argument is not required to exist in
    `param.kwargs`; None is then inserted into `safe.kwargs`. Otherwise
    a missing value raises an exception."""
    _validate_one(argname, param, safe, _check_tarfile, optional, safe.kwargs[hashkey], safe.kwargs['newchecksum'])

def validate_file(argname, param, safe, hashkey, optional=False):
    """Validates that an argument is a file and matches the hashkey.

    Checks that an argument named `argname` exists in `param.kwargs`
    and it is a tar file which matches the provided hashkey. If
    successful the string is copied into `safe.kwargs` and the value
    is removed from `param.kwargs`.

    If `optional` is True, the argument is not required to exist in
    `param.kwargs`; None is then inserted into `safe.kwargs`. Otherwise
    a missing value raises an exception."""
    _validate_one(argname, param, safe, _check_file, optional)
