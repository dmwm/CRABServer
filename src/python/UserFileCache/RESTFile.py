# WMCore dependecies here
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Validation import validate_str, _validate_one, validate_num
from WMCore.REST.Error import RESTError, InvalidParameter, NoSuchInstance
from WMCore.REST.Format import RawFormat, JSONFormat, XMLFormat

# CRABServer dependecies here
from UserFileCache.RESTExtensions import _check_file, ChecksumFailed, validate_file, authz_login_valid

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
            validate_file("inputfile", param, safe, 'hashkey',  optional=False)
            validate_str("inputfilename", param, safe, RX_FILENAME, optional=True)
        if method in ['GET']:
            validate_str("hashkey", param, safe, RX_HASH, optional=True)
            validate_str("inputfilename", param, safe, RX_FILENAME, optional=True)
            validate_num("nodownload", param, safe, optional=True)
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
        outfilepath = None
        outfilename = None
        result = {'hashkey': hashkey}
        if inputfilename:
            # setting the subpath with the user name and filename as requested
            outfilepath = os.path.join(self.cachedir, cherrypy.request.user['login'])
            outfilename = os.path.join(outfilepath, inputfilename)
            result['name'] = inputfilename
        else:
            # using the hash of the file to create a subdir and filename
            outfilepath = os.path.join(self.cachedir, hashkey[0:2])
            outfilename = os.path.join(outfilepath, hashkey)

        if os.path.isfile(outfilename) and not inputfilename:
            # we do not want to upload again a file that already exists
            self.__touch(outfilename)
            result['size'] = os.path.getsize(outfilename)
        else:
            if not os.path.isdir(outfilepath):
                os.makedirs(outfilepath)
            handlefile = open(outfilename,'wb')
            inputfile.file.seek(0)
            shutil.copyfileobj(inputfile.file, handlefile)
            handlefile.close()
            result['size'] = os.path.getsize(outfilename)
        return [result]

    @restcall(formats = [ ('application/json', JSONFormat()),
                          ('application/xml', XMLFormat('UFCAPIs')),
                          ('application/octet-stream', RawFormat())
                        ])
    def get(self, hashkey, inputfilename, nodownload):
        """Retrieve a file previously uploaded to the local filesystem.
           The base path on the local filesystem is configurable.
           If the `nodownload` argument is true or different from zero instead of
           returning the raw file, summary information is returned.

           The caller needs to be a CMS user with a valid CMS x509 cert/proxy.

           :arg str hashkey: the sha256 hexdigest of the file, calculated over the tuple
                             (name, size, mtime, uname) of all the tarball members
           :arg str inputfilename: in case its needed to retrieve a file with a specific
                                   name
           :arg int nodownload: flag used to get just summary informtion
           :return: hashkey, name, size of the requested file or the raw file if
                    `nodownload` is False."""
        result = {}
        filename = None
        if hashkey:
            # defining the path/name from the hash of the file
            filename = os.path.join(self.cachedir, hashkey[0:2], hashkey)
            result['hashkey'] = hashkey
        elif inputfilename:
            # composing the path/name from the user name and the input file
            filename = os.path.join(self.cachedir, cherrypy.request.user['login'], inputfilename)
            tar = tarfile.open(filename, mode='r')
            lsl = [(x.name, int(x.size), int(x.mtime), x.uname) for x in tar.getmembers()]
            realhashkey = hashlib.sha256(str(lsl)).hexdigest()
            result['hashkey'] = realhashkey
            result['name'] = inputfilename
        if not os.path.isfile(filename):
            raise NoSuchInstance("Not found")
        self.__touch(filename)
        if not nodownload:
            return serve_file(filename, "application/octet-stream", "attachment")
        result['exists'] = True
        result['size'] = os.path.getsize(filename)
        return [result]

    def __touch(self, filename):
        """Touch the file to keep automated cleanup away

        :arg str filename: the filename path."""
        if os.path.isfile(filename):
            os.utime(filename, None)
