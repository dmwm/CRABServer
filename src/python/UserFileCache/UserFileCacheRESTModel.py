#! /usr/bin/env python

"""
REST Model portion for User File Cache.
"""

import os
import shutil
import hashlib

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

        self._addMethod('GET', 'status', self.status)

        # Manually add a file upload method because we don't want validation on the input arguments

        if not self.methods.has_key('POST'):
            self.methods['POST'] = {}
        self.methods['POST']['upload'] = {'args':       ['userfile'],
                                          'call':       self.upload,
                                          'validation': [],
                                          'version':    1,
                                          'expires':    self.defaultExpires}


    def status(self):
        """
        Trivial method to query if the server is up
        """

        return {'up': True}


    @restexpose
    def upload(self, userfile):
        """
        Upload the file, calculating the hash renaming it to the
        hash value. If the file already exists, just touch it
        """

        hasher = hashlib.sha256()


        size = 0
        while True:
            data = userfile.file.read(8192)
            if not data:
                break
            hasher.update(data)
            size += len(data)
        digest = hasher.hexdigest()
        fileDir = os.path.join(self.cacheDir, digest[0:2])
        fileName = os.path.join(fileDir, digest)

        if os.path.isfile(fileName):
            os.utime(fileName, None) # "touch" the file
        else:
            if not os.path.isdir(fileDir):
                os.makedirs(fileDir)
            handle = open(fileName,'wb')
            userfile.file.seek(0)
            shutil.copyfileobj(userfile.file, handle)

        return {'size':size, 'name': userfile.filename, 'id' : hasher.hexdigest()}

