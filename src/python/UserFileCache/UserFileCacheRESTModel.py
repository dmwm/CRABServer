#! /usr/bin/env python

"""
REST Model portion for User File Cache.
"""

import cherrypy
import hashlib
import os
import shutil
import tarfile

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
                       args=['hashkey'], validation=[])
        self._addMethod('GET', 'status', self.status)

        # Manually add a file upload method because we don't want validation on the input arguments

        if not self.methods.has_key('POST'):
            self.methods['POST'] = {}
        self.methods['POST']['upload'] = {'args':       ['userfile', 'checksum'],
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


    def exists(self, hashkey):
        """
        Does the file already exist on the server?
        """
        try:
            if len(hashkey) < 16:
                raise cherrypy.NotFound
            int(hashkey, 16)
        except ValueError:
            raise cherrypy.NotFound

        fileName = os.path.join(self.cacheDir, hashkey[0:2], hashkey)
        if os.path.isfile(fileName):
            self.touch(hashkey)
            size = os.path.getsize(fileName)
            url = 'http://%s:%s/userfilecache/download?hashkey=%s' % (self.host, self.port, hashkey)
            return {'exists': True, 'size':size, 'hashkey':hashkey, 'url':url}

        return {'exists': False}


    @restexpose
    def upload(self, userfile, checksum):
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

        fileDir = os.path.join(self.cacheDir, digest[0:2])
        fileName = os.path.join(fileDir, digest)

        # Basic preservation of the file integrity
        if not (digest == checksum):
            raise cherrypy.HTTPError(400, 'File transfer error: digest check failed.')

        if os.path.isfile(fileName):
            self.touch(digest)
        else:
            if not os.path.isdir(fileDir):
                os.makedirs(fileDir)
            handle = open(fileName,'wb')
            userfile.file.seek(0)
            shutil.copyfileobj(userfile.file, handle)
            handle.close()
            size = os.path.getsize(fileName)

        url = 'http://%s:%s/userfilecache/download?hashkey=%s' % (self.host, self.port, digest)

        return {'size':size, 'name':userfile.filename, 'hashkey':digest, 'url':url}

    def touch(self, digest):
        """
        Touch the file to keep automated cleanup away
        """

        fileName = os.path.join(self.cacheDir, digest[0:2], digest)
        #fileName = os.path.join(fileDir, digest)
        if os.path.isfile(fileName):
            os.utime(fileName, None)

        return
