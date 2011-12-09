#! /usr/bin/env python

"""
Testing of the UserFileCache
"""

import commands
import filecmp
import hashlib
import json
import logging
import os
import shutil
import tarfile
import tempfile
import time
import unittest
import urllib

from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTClientAPI import makeRequest
from WMQuality.WebTools.RESTClientAPI import methodTest
from WMQuality.WebTools.RESTServerSetup import DefaultConfig

testCacheDir   = '/tmp/UnitTestCacheDir'
testInputName  = '/tmp/UnitTestInputFile'
testOutputName = '/tmp/UnitTestOutputFile'
testTarName    = '/tmp/test.tgz'

uploadDir = 'ewv'
uploadName = 'test_topublish.tgz'

class UserFileCacheConfig(DefaultConfig):
    """
    Default config for unit test environment
    """
    def setupRequestConfig(self):
        """
        Tweak the config for running the tests
        """

        #pylint: disable-msg=E1101
        self.UnitTests.views.active.rest.workloadDBName = "test"
        self.UnitTests.views.active.rest.security_roles = []
        self.UnitTests.views.active.rest.userCacheDir = testCacheDir
        self.UnitTests.views.active.rest.componentDir = '/tmp/'

        download = self.UnitTests.views.active.section_('download')
        download.object = 'UserFileCache.UserFileCachePage'
        download.templates =  '/tmp'
        download.userCacheDir = testCacheDir
        #pylint: enable-msg=E1101



class TestUserFileCache(RESTBaseUnitTest):
    """
    Testing of the UserFileCache
    """
    def initialize(self):
        """
        Tweak the config for running the tests
        """
        #pylint: disable-msg=E1101
        self.config = UserFileCacheConfig('UserFileCache.UserFileCacheRESTModel')
        self.config.setFormatter('WMCore.WebTools.RESTFormatter')
        self.config.setupRequestConfig()

        self.config.UnitTests.object = 'UserFileCache.UserFileCacheRESTModel'
        self.config.UnitTests.views.active.rest.logLevel = 'DEBUG'

        self.config.Webtools.environment = 'development'
        self.config.Webtools.error_log_level = logging.ERROR
        self.config.Webtools.access_log_level = logging.ERROR
        self.config.Webtools.host = '127.0.0.1'
        self.config.Webtools.port = 8588
        #pylint: enable-msg=E1101

        self.urlbase = self.config.getServerUrl()


    def setUp(self):
        """
        Do the setup
        """

        RESTBaseUnitTest.setUp(self)
        # Make the test files

        try:
            os.unlink(testInputName)
        except OSError:
            pass

        with open(testInputName, 'w') as testFile:
            testFile.write(str(time.time()))
            testFile.write('\nFirst line\n')
            [testFile.write(str(x)) for x in xrange(0,1000)]
            testFile.write('\nLast line\n')

        (status, output) = commands.getstatusoutput('tar cfz %s %s' % (testTarName, testInputName))

        # Figure hash of file
        tar = tarfile.open(name=testTarName, mode='r:gz')
        lsl = [(x.name, int(x.size), int(x.mtime), x.uname) for x in tar.getmembers()]
        hasher = hashlib.sha256(str(lsl))

        self.hexDigest = hasher.hexdigest()

        self.host = 'http://%s:%s' % (self.config.Webtools.host, self.config.Webtools.port)
        return


    def tearDown(self):
        """
        Try to clean up. We don't care if it fails
        """

        #pylint: disable-msg=W0704
        try:
            shutil.rmtree(testCacheDir)
        except OSError:
            pass
        try:
            os.unlink(testInputName)
        except OSError:
            pass
        try:
            os.unlink(testOutputName)
        except OSError:
            pass
        try:
            os.unlink(testTarName)
        except OSError:
            pass
        #pylint: enable-msg=W0704


    def testDownload(self):
        """
        Upload the file again since it's deleted
        then make sure file content and size are the same
        """

        returnDict = self.testUpload()

        opener = urllib.FancyURLopener()
        url = '%s/unittests/download?hashkey=%s' % (self.host, returnDict['hashkey'])
        opener.retrieve(url, testOutputName)

        self.assertEqual(os.path.getsize(testOutputName), os.path.getsize(testTarName))
        self.assertTrue(filecmp.cmp(testOutputName, testTarName))

        return


    def testUpload(self):
        """
        Test uploading with curl
        Make sure size returned by server is what we expect
        """

        with tempfile.NamedTemporaryFile() as tmpFile:
            url = self.urlbase + 'upload'
            curlCommand = 'curl -H "Accept: application/json" -F checksum=%s -F userfile=@%s %s -o %s' % \
                          (self.hexDigest, testTarName, url, tmpFile.name)
            (status, output) = commands.getstatusoutput(curlCommand)

            self.assertEqual(status, 0, 'Upload failed with output %s' % output)
            returnDict = json.loads(tmpFile.read())
            self.assertEqual(returnDict['size'], os.path.getsize(testTarName))

        return returnDict


    def testStatus(self):
        """
        Test the simple status method to see if the server is there
        """

        verb = 'GET'
        url = self.urlbase + 'status'
        expected = json.dumps({"up":True})
        output = {'code':200, 'type':'text/json', 'data':expected}
        expireTime = 0

        methodTest(verb, url, output=output, expireTime=expireTime)


    def testExists(self):
        """
        Test the exists function
        """
        verb = 'GET'
        existsUrl = self.urlbase + 'exists'
        uploadUrl = self.urlbase + 'upload'
        expireTime = 0

        # This one can't exist, too short
        requestInput = {'hashkey':'fffffffffffffffffffff'}
        expected = json.dumps({"exists":False})
        output = {'code':200, 'type':'text/json', 'data':expected}
        methodTest(verb, existsUrl, output=output, expireTime=expireTime, request_input=requestInput)

        # Upload the file
        with tempfile.NamedTemporaryFile() as curlOutput:
            curlCommand = 'curl -H "Accept: application/json" -F checksum=%s -F userfile=@%s %s -o %s' % \
                          (self.hexDigest, testTarName, uploadUrl, curlOutput.name)
            (status, output) = commands.getstatusoutput(curlCommand)
            self.assertEqual(status, 0, 'Problem uploading with curl')
            returnDict = json.loads(curlOutput.read())

        # Now re-test exists
        requestInput = {'hashkey':returnDict['hashkey']}
        data, code, contentType, response = makeRequest(existsUrl, values=requestInput,
                                  verb=verb, accept='application/json', contentType='application/json')
        jsonData = json.loads(data)
        self.assertEqual(code, 200)
        self.assertTrue(jsonData['exists'])
        self.assertEqual(self.hexDigest, returnDict['hashkey'])

        return


    def testWithName(self):
        """
        Test uploading and retrieving file with a name rather than hashkey
        """

        verb = 'GET'
        existsUrl = self.urlbase + 'exists'
        uploadUrl = self.urlbase + 'upload'
        expireTime = 0

        # Test uploading file
        with tempfile.NamedTemporaryFile() as tmpFile:
            url = self.urlbase + 'upload'
            curlCommand = 'curl -H "Accept: application/json" -F subDir=%s -F name=%s -F checksum=%s -F userfile=@%s %s -o %s' % \
                          (uploadDir, uploadName, self.hexDigest, testTarName, url, tmpFile.name)

            (status, output) = commands.getstatusoutput(curlCommand)

            self.assertEqual(status, 0, 'Upload failed with output %s' % output)
            returnDict = json.loads(tmpFile.read())
            self.assertEqual(returnDict['size'], os.path.getsize(testTarName))

        # Test exists
        requestInput = {'name' : uploadName, 'subDir' : uploadDir}
        data, code, contentType, response = makeRequest(existsUrl, values=requestInput,
                                  verb=verb, accept='application/json', contentType='application/json')
        jsonData = json.loads(data)
        self.assertEqual(code, 200, 'Failed with output %s' % data)
        self.assertTrue(jsonData['exists'])
        self.assertEqual(self.hexDigest, returnDict['hashkey'])

        # Test download
        opener = urllib.FancyURLopener()
        url = '%s/unittests/download?subDir=%s;name=%s' % (self.host, uploadDir, uploadName)
        opener.retrieve(url, testOutputName)

        self.assertEqual(os.path.getsize(testOutputName), os.path.getsize(testTarName))
        self.assertTrue(filecmp.cmp(testOutputName, testTarName))

        return



if __name__ == '__main__':
    unittest.main()
