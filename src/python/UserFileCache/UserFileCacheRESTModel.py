#!/usr/bin/env python

"""
REST Model portion for User File Cache.
"""

import cherrypy
import hashlib
import os
import shutil
import tarfile

from WMCore.Lexicon import hnName
from WMCore.Lexicon import check as regexCheck
from WMCore.WebTools.RESTModel import restexpose
from WMCore.WebTools.RESTModel import RESTModel

class UserFileCacheRESTModel(RESTModel):
    """
    A REST Model for User File Cache.
    """
    def __init__(self, config = None):
        if config is None:
            config = {}
        RESTModel.__init__(self, config)

        self.cacheDir = getattr(config, 'userCacheDir', '/tmp/UserFileCache')

        self._addMethod('GET', 'exists', self.exists,
                       args=['hashkey', 'subDir', 'name'], validation=[])
        self._addMethod('GET', 'status', self.status)

        # Manually add a file upload method because we don't want validation on the input arguments
        if not self.methods.has_key('POST'):
            self.methods['POST'] = {}
        self.methods['POST']['upload'] = {'args':       ['userfile', 'checksum', 'subDir', 'name'],
                                          'call':       self.upload,
                                          'validation': [],
                                          'version':    1,
                                          'expires':    self.defaultExpires}

        # These things are not in the config for unit tests
        self.host = 'localhost'
        self.port = 0
        try:
            self.host = self.config.Webtools.host
            self.port = self.config.Webtools.port
        except AttributeError:
            pass

    def status(self):
        """
        Trivial method to query if the server is up
        """

        return {'up': True}


    def exists(self, hashkey=None, subDir=None, name=None):
        """
        Does the file already exist on the server?
        """
        # TODO: new API specifying name, dir (hn name)
        if hashkey:
            try:
                if len(hashkey) < 16:
                    raise cherrypy.NotFound
                int(hashkey, 16)
            except ValueError:
                raise cherrypy.NotFound
            fileName = os.path.join(self.cacheDir, hashkey[0:2], hashkey)
            url = 'http://%s:%s/userfilecache/download?hashkey=%s' % (self.host, self.port, hashkey)
        else:
            hnName(subDir)
            regexCheck(r'^[a-zA-Z0-9\-_\.]+$', name)
            fileName = os.path.join(self.cacheDir, subDir, name)
            url = 'http://%s:%s/userfilecache/download?subDir=%s;name=%s' % (self.host, self.port, subDir, name)

            tar = tarfile.open(fileName, mode='r')
            lsl = [(x.name, int(x.size), int(x.mtime), x.uname) for x in tar.getmembers()]
            haskey = hashlib.sha256(str(lsl)).hexdigest()

        if os.path.isfile(fileName):
            self.touch(fileName)
            size = os.path.getsize(fileName)
            return {'exists': True, 'size':size, 'hashkey':hashkey, 'url':url}

        return {'exists': False}


    @restexpose
    def upload(self, userfile, checksum, subDir=None, name=None):
        """
        Upload the file, calculating the hash renaming it to the
        hash value. If the file already exists, just touch it
        """
        try:
            tar = tarfile.open(fileobj=userfile.file, mode='r')
            lsl = [(x.name, int(x.size), int(x.mtime), x.uname) for x in tar.getmembers()]
            hasher = hashlib.sha256(str(lsl))
            digest = hasher.hexdigest()
        except tarfile.ReadError:
            raise cherrypy.HTTPError(400, 'File is not a .tgz file.')

        if subDir and name:
            hnName(subDir)
            regexCheck(r'^[a-zA-Z0-9\-_\.]+$', name)
            fileDir = os.path.join(self.cacheDir, subDir)
            fileName = os.path.join(fileDir, name)
            url = 'http://%s:%s/userfilecache/download?subDir=%s;name=%s' % (self.host, self.port, subDir, name)
        elif checksum:
            fileDir = os.path.join(self.cacheDir, digest[0:2])
            fileName = os.path.join(fileDir, digest)
            url = 'http://%s:%s/userfilecache/download?hashkey=%s' % (self.host, self.port, digest)
        else:
            raise cherrypy.HTTPError(400, 'Must supply name and directory or checksum.')

        # Basic preservation of the file integrity
        if digest != checksum:
            msg = "File transfer error: digest check failed between %s and %s"  % (digest, checksum)
            raise cherrypy.HTTPError(400, msg)

        if os.path.isfile(fileName) and not (subDir and name):
            self.touch(fileName)
            size = os.path.getsize(fileName)
        else:
            if not os.path.isdir(fileDir):
                os.makedirs(fileDir)
            handle = open(fileName,'wb')
            userfile.file.seek(0)
            shutil.copyfileobj(userfile.file, handle)
            handle.close()
            size = os.path.getsize(fileName)

        return {'size':size, 'hashkey':digest, 'url':url}

    def touch(self, fileName):
        """
        Touch the file to keep automated cleanup away
        """
        if os.path.isfile(fileName):
            os.utime(fileName, None)

        return
