# WMCore dependecies here
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Validation import validate_str, _validate_one, validate_num
from WMCore.REST.Error import RESTError, InvalidParameter, MissingObject
from WMCore.REST.Format import RawFormat

# CRABServer dependecies here
from UserFileCache.RESTExtensions import _check_file, ChecksumFailed, validate_file, authz_login_valid, quota_user_free

# external dependecies here
import cherrypy
from cherrypy.lib.static import serve_file
import re
import tarfile
import hashlib
import os
import shutil

# here go the all regex to be used for validation
RX_HASH = re.compile(r'^[a-f0-9]{64}$')
# input file name may correspond to workflowname + _publish.tgz
RX_FILENAME = re.compile(r'^[a-zA-Z0-9\.\-_]{1,80}_publish\.tgz$')

def touch(filename):
    """Touch the file to keep automated cleanup away

    :arg str filename: the filename path."""
    if os.path.isfile(filename):
        os.utime(filename, None)

def filepath(cachedir):
    # NOTE: if we need to share a file between users (something we do not really want to make default or too easy...) we can:
    #         - use the group of the user instead of the user name, which can be retrieved from cherrypy.request.user
    #         - have an extra input parameter group=something (but this wouldn't be transparent when downloading it)
    return os.path.join(cachedir, cherrypy.request.user['login'][0], cherrypy.request.user['login'])

class RESTFile(RESTEntity):
    """The RESTEntity for uploaded and downloaded files"""

    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        self.cachedir = config.cachedir

    def validate(self, apiobj, method, api, param, safe):
        """Validating all the input parameter as enforced by the WMCore.REST module"""
        authz_login_valid()

        if method in ['PUT']:
            validate_str("hashkey", param, safe, RX_HASH, optional=False)
            validate_file("inputfile", param, safe, 'hashkey', optional=False)
            validate_str("inputfilename", param, safe, RX_FILENAME, optional=True)
        if method in ['GET']:
            validate_str("hashkey", param, safe, RX_HASH, optional=True)
            validate_str("inputfilename", param, safe, RX_FILENAME, optional=True)
            if not safe.kwargs['hashkey'] and not safe.kwargs['inputfilename']:
                raise InvalidParameter("Missing input parameter")

    @restcall
    def put(self, inputfile, hashkey, inputfilename):
        """Allow to upload a tarball file to be written in the local filesystem.
           Base path of the local filesystem is configurable.

           The caller needs to be a CMS user with a valid CMS x509 cert/proxy.

           :arg file inputfile: file object to be uploaded
           :arg str hashkey: the sha256 hexdigest of the file, calculated over the tuple
                             (name, size, mtime, uname) of all the tarball members
           :arg str inputfilename: in case the file name needs to be specific
           :return: hashkey, name, size of the uploaded file."""
        outfilepath = filepath(self.cachedir)
        outfilename = None
        result = {'hashkey': hashkey}
        if inputfilename:
            # setting the subpath with the user name and filename as requested
            outfilename = os.path.join(outfilepath, inputfilename)
            result['name'] = inputfilename
        else:
            # using the hash of the file to create a subdir and filename
            outfilepath = os.path.join(outfilepath, hashkey[0:2])
            outfilename = os.path.join(outfilepath, hashkey)

        if os.path.isfile(outfilename) and not inputfilename:
            # we do not want to upload again a file that already exists
           touch(outfilename)
           result['size'] = os.path.getsize(outfilename)
        else:
            # check that the user quota is still below limit
            quota_user_free(filepath(self.cachedir), inputfile)

            if not os.path.isdir(outfilepath):
                os.makedirs(outfilepath)
            handlefile = open(outfilename,'wb')
            inputfile.file.seek(0)
            shutil.copyfileobj(inputfile.file, handlefile)
            handlefile.close()
            result['size'] = os.path.getsize(outfilename)
        return [result]

    @restcall(formats = [('application/octet-stream', RawFormat())])
    def get(self, hashkey, inputfilename):
        """Retrieve a file previously uploaded to the local filesystem.
           The base path on the local filesystem is configurable.

           The caller needs to be a CMS user with a valid CMS x509 cert/proxy.

           :arg str hashkey: the sha256 hexdigest of the file, calculated over the tuple
                             (name, size, mtime, uname) of all the tarball members
           :arg str inputfilename: in case its needed to retrieve a file with a specific
                                   name
           :return: the raw file"""
        filename = None
        infilepath = filepath(self.cachedir)
        if hashkey:
            # defining the path/name from the hash of the file
            filename = os.path.join(infilepath, hashkey[0:2], hashkey)
        elif inputfilename:
            # composing the path/name from the user name and the input file
            filename = os.path.join(infilepath, inputfilename)
        if not os.path.isfile(filename):
            raise MissingObject("Not such file")
        touch(filename)
        return serve_file(filename, "application/octet-stream", "attachment")


class RESTFileInfo(RESTEntity):
    """The RESTEntity to get information about uploaded files"""

    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        self.cachedir = config.cachedir

    def validate(self, apiobj, method, api, param, safe):
        """Validating all the input parameter as enforced by the WMCore.REST module"""
        authz_login_valid()
        if method in ['GET']:
            validate_str("hashkey", param, safe, RX_HASH, optional=True)
            validate_str("inputfilename", param, safe, RX_FILENAME, optional=True)
            if not safe.kwargs['hashkey'] and not safe.kwargs['inputfilename']:
                raise InvalidParameter("Missing input parameter")

    @restcall
    def get(self, hashkey, inputfilename):
        """Retrieve the file summary information.

           The caller needs to be a CMS user with a valid CMS x509 cert/proxy.

           :arg str hashkey: the sha256 hexdigest of the file, calculated over the tuple
                             (name, size, mtime, uname) of all the tarball members
           :arg str inputfilename: in case its needed to retrieve a file with a specific
                                   name
           :return: hashkey, name, size of the requested file"""
        result = {}
        filename = None
        infilepath = filepath(self.cachedir)
        if hashkey:
            # defining the path/name from the hash of the file
            filename = os.path.join(infilepath, hashkey[0:2], hashkey)
            result['hashkey'] = hashkey
        elif inputfilename:
            # composing the path/name from the user name and the input file
            filename = os.path.join(infilepath, inputfilename)
            tar = tarfile.open(filename, mode='r')
            lsl = [(x.name, int(x.size), int(x.mtime), x.uname) for x in tar.getmembers()]
            realhashkey = hashlib.sha256(str(lsl)).hexdigest()
            result['hashkey'] = realhashkey
            result['name'] = inputfilename
        if not os.path.isfile(filename):
            raise MissingObject("Not such file")
        touch(filename)
        result['exists'] = True
        result['size'] = os.path.getsize(filename)
        return [result]
