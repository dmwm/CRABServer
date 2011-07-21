#!/usr/bin/env/ python
#pylint: disable=E1101
"""
_CRABRESTModel_t_

"""

import commands
import json
import unittest
import logging
import os
import tempfile
import time
import hashlib

from WMQuality.WebTools.RESTClientAPI import methodTest
from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTServerSetup import DefaultConfig
from WMCore.Cache.WMConfigCache import ConfigCache
from WMCore.Database.CMSCouch import CouchServer
from WMCore.FwkJobReport.Report import Report
try:
    from CRABServer.CRABRESTModel import getJobsFromRange
except ImportError:
    from CRABRESTModel import getJobsFromRange
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import allSoftwareVersions
import WMCore.RequestManager.RequestDB.Interface.Admin.SoftwareManagement as SoftwareAdmin

databaseURL = os.getenv("DATABASE")
databaseSocket = os.getenv("DBSOCK")

couchURL = os.getenv("COUCHURL")
workloadDB = 'workload_db_test'
configCacheDB = 'config_cache_test'
jsmCacheDB = 'jsmcache_test'

doUpload = 0 # Change to 1 if you have a UserFileCache server running and configured

class CRABRESTModelTest(RESTBaseUnitTest):
    """
    _CRABRESTModel_ test
    """
    psetTweaks = '{"process": {"maxEvents": {"parameters_": ["input"], "input": 10}, \
"outputModules_": ["output"], "parameters_": ["outputModules_"], \
"source": {"parameters_": ["fileNames"], "fileNames": []}, \
"output": {"parameters_": ["fileName"], "fileName": "outfile.root"}, \
"options": {"parameters_": ["wantSummary"], "wantSummary": true}}}'

    confFile = '''import FWCore.ParameterSet.Config as cms
process = cms.Process("Slurp")

process.source = cms.Source("PoolSource", fileNames = cms.untracked.vstring())
process.maxEvents = cms.untracked.PSet( input       = cms.untracked.int32(10) )
process.options   = cms.untracked.PSet( wantSummary = cms.untracked.bool(True) )

process.output = cms.OutputModule("PoolOutputModule",
    outputCommands = cms.untracked.vstring("drop *", "keep recoTracks_*_*_*"),
    fileName = cms.untracked.string("outfile.root"),
)
process.out_step = cms.EndPath(process.output)'''

    insConfParams = {
        "Group" : "Analysis",
        "Team" : "Analysis",
        "UserDN" : "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=mmascher/CN=720897/CN=Marco Mascheroni",
        "ConfFile" : confFile,
        "PsetTweaks" : psetTweaks, #json
        "PsetHash" : "21cb400c6ad63c3a97fa93f8e8785127", #edmhash
        "Label" : "the label",
        "Description" : "the description"
    }

    insUserParams = {
        "Group" : "Analysis",
        "UserDN" : "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=mmascher/CN=720897/CN=Marco Mascheroni",
        "Team" : "Analysis",
        "Email" : "marco.mascheroni@cern.ch"
    }

    postReqParams = {
        "Username": "mmascher",
        "RequestorDN": "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=mmascher/CN=720897/CN=Marco Mascheroni",
        "outputFiles": [
            "out.root"
        ],
        "Group": "Analysis",
        "RequestType": "Analysis",
        "InputDataset": "/RelValProdTTbar/JobRobot-MC_3XY_V24_JobRobot-v1/GEN-SIM-DIGI-RECO",
        "JobSplitAlgo": "FileBased",
        "ProcessingVersion": "",
        "AnalysisConfigCacheDoc": "_",
        "Requestor": "mmascher",
        "DbsUrl": "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet",
        "ScramArch": "slc5_ia32_gcc434",
        "JobSplitArgs": {
            "files_per_job": 100
        },
        "RequestName": "crab_MyAnalysis__",
        "Team": "Analysis",
        "asyncDest": "T2_IT_Bari",
        "CMSSWVersion": "CMSSW_3_9_7"
    }

    dataLocParams = {
        "requestID" : "mmascher_crab_MyAnalysis___110429_030846",
        "jobRange" : '1,2'
    }

    logLocParams = {
        "requestID" : "mmascher_crab_MyAnalysis___110506_123756",
        "jobRange" : '1,2'
    }

    reportParams = {
        "requestID" : "mmascher_crab_MyAnalysis___110506_123756",
    }

    outpfn = 'srm://srmcms.pic.es:8443/srm/managerv2?SFN=/pnfs/pic.es/data/cms/store/user/mmascher/RelValProdTTbar/' + \
             '1304039730//0000/4C86B480-0D72-E011-978B-002481CFE25E.root'
    logpfn = 'srm://storm-se-01.ba.infn.it:8444/srm/managerv2?SFN=/cms//store/unmerged/logs/prod/2011/5/6/' + \
             'mmascher_crab_MyAnalysis___110506_123756/Analysis/0000/0/f56e599e-77cc-11e0-b51e-0026b958c394-99-0-logArchive.tar.gz'


    def initialize(self):
        """
            Initialize the class.
        """
        self.config = DefaultConfig('CRABRESTModel')
        self.config.Webtools.environment = 'development'
        self.config.Webtools.error_log_level = logging.DEBUG
        self.config.Webtools.access_log_level = logging.DEBUG
        self.config.Webtools.port = 8588

        #DB Parameters used by RESTServerSetup
        self.config.UnitTests.views.active.rest.database.connectUrl = databaseURL
        self.config.UnitTests.views.active.rest.database.socket = databaseSocket
        #DB Parameters used by
        self.config.UnitTests.section_('database')
        self.config.UnitTests.database.connectUrl = databaseURL
        self.config.UnitTests.database.socket = databaseSocket
        self.config.UnitTests.object = 'CRABRESTModel'
        self.config.UnitTests.views.active.rest.model.couchUrl = couchURL
        self.config.UnitTests.views.active.rest.model.workloadCouchDB = workloadDB
        self.config.UnitTests.views.active.rest.configCacheCouchURL = couchURL
        self.config.UnitTests.views.active.rest.configCacheCouchDB = configCacheDB
        self.config.UnitTests.views.active.rest.jsmCacheCouchURL = couchURL
        self.config.UnitTests.views.active.rest.jsmCacheCouchDB = jsmCacheDB
        self.config.UnitTests.views.active.rest.agentDN = ''
        self.config.UnitTests.views.active.rest.SandBoxCache_endpoint = 'cms-xen39.fnal.gov'
        self.config.UnitTests.views.active.rest.SandBoxCache_port = 7739
        self.config.UnitTests.views.active.rest.SandBoxCache_basepath = 'userfilecache/userfilecache'
        self.config.UnitTests.views.active.rest.logLevel = 'DEBUG'

        self.schemaModules = ['WMCore.RequestManager.RequestDB']
        self.urlbase = self.config.getServerUrl()


    #Override setup to add software versions
    def setUp(self):
        """
        _setUp_
        """
        data_path = os.path.join(os.path.dirname(__file__), '../data')
        self.test_data_dir = os.path.normpath(data_path)

        RESTBaseUnitTest.setUp(self)
        self.testInit.setupCouch(workloadDB)
        self.testInit.setupCouch(configCacheDB, "ConfigCache")

        self.testInit.setupCouch(jsmCacheDB + "/fwjrs", "FWJRDump")
        self.testInit.setupCouch(jsmCacheDB + "/jobs", "JobDump")



        for v in allSoftwareVersions():
            SoftwareAdmin.addSoftware(v)


    def tearDown(self):
        """
        _tearDown_
        """
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()


    def insertConfig(self):
        """
        _insertConfig_
        """
        api = "config"

        jsonString = json.dumps(self.insConfParams, sort_keys=False)
        result, exp = methodTest('POST', self.urlbase + api, jsonString, 'application/json', \
                                 'application/json', {'code' : 200})
        self.assertTrue(exp is not None)

        return json.loads(result)


    def testPostUserConfig(self):
        """
        _testPostUserConfig_
        """
        result = self.insertConfig()

        self.assertTrue( result.has_key("DocID") )
        self.assertTrue( result.has_key("DocRev") )
        self.assertTrue( len(result["DocID"])>0 )
        self.assertTrue( len(result["DocRev"])>0 )

        #chek if document result["DocID"] is in couch
        confCache = ConfigCache(self.config.UnitTests.views.active.rest.configCacheCouchURL, \
                    self.config.UnitTests.views.active.rest.configCacheCouchDB)
        #and contains the PSet
        confCache.loadByID(result["DocID"])
        self.assertTrue( len(confCache.getPSetTweaks())>0)


    def insertUser(self):
        """
        _insertUser_
        """
        api = "user"

        jsonString = json.dumps(self.insUserParams, sort_keys=False)
        result, exp = methodTest('POST', self.urlbase + api, jsonString, 'application/json', \
                                 'application/json', {'code' : 200})
        self.assertTrue(exp is not None)
        return json.loads(result)


    def testAddUser(self):
        """
        _testAddUser_
        """

        result = self.insertUser()

        self.assertTrue(result.has_key("group"))
        self.assertTrue(result.has_key("hn_name"))
        self.assertTrue(result.has_key("team"))

        self.assertEqual(result["team"] , "Analysis registered")
        self.assertEqual(result["hn_name"] , "mmascher")
        self.assertEqual(result["group"] , "'Analysis'egistered")


    def testPostRequest(self):
        """
        _testPostRequest_
        """
        api = "task/%s" % self.postReqParams['RequestName']

        #_insertConfig has been already tested in the previous test method
        result = self.insertConfig()
        self.postReqParams['AnalysisConfigCacheDoc'] = result['DocID']

        #Posting a request without registering the user first
        jsonString = json.dumps(self.postReqParams, sort_keys=False)
        result, exp = methodTest('POST', self.urlbase + api, jsonString, \
                        'application/json', 'application/json', {'code' : 500})
        self.assertTrue(exp is not None)

        #Again, insertUser tested before. It should work
        self.insertUser()

        result, exp = methodTest('POST', self.urlbase + api, jsonString, \
                        'application/json', 'application/json', {'code' : 200})
        self.assertTrue(exp is not None)
        result = json.loads(result)
        self.assertTrue(result.has_key("ID"))
        #SINCE python 2.7 :(
        #self.assertRegexpMatches(result['ID'], \
        #           "mmascher_crab_MyAnalysis__\d\d\d\d\d\d_\d\d\d\d\d\d")
        self.assertTrue(result['ID'].startswith("%s_%s" % (self.postReqParams["Username"], \
                                          self.postReqParams["RequestName"])))

        # Test various problems with asyncDest
        self.postReqParams['asyncDest'] = 'T2_US_Bari'
        jsonString = json.dumps(self.postReqParams, sort_keys=False)
        result, exp = methodTest('POST', self.urlbase + api, jsonString, \
                        'application/json', 'application/json', {'code' : 500})
        self.assertTrue(result.find('not a valid CMS site') > 1)

        self.postReqParams['asyncDest'] = 'Bari'
        jsonString = json.dumps(self.postReqParams, sort_keys=False)
        result, exp = methodTest('POST', self.urlbase + api, jsonString, \
                        'application/json', 'application/json', {'code' : 500})
        self.assertTrue(result.find('not a valid CMS site name') > 1)

        del self.postReqParams['asyncDest']
        jsonString = json.dumps(self.postReqParams, sort_keys=False)
        result, exp = methodTest('POST', self.urlbase + api, jsonString, \
                        'application/json', 'application/json', {'code' : 500})
        self.assertTrue(result.find('asyncDest parameter is missing') > 1)


    def injectFWJR(self, reportXML, jobID, retryCount, taskName, skipoutput = False):
        """
        Inject a fake fwjr with a cmsRun1 section into couchDB
        """
        couchServer = CouchServer(os.environ["COUCHURL"])

        self._injectJobReport(couchServer, taskName, jobID)

        fwjrdb = couchServer.connectDatabase(jsmCacheDB + "/fwjrs")

        myReport = Report("cmsRun1")
        myReport.parse(reportXML)
        myReport.setTaskName(taskName)
        myReport.data.cmsRun1.status = 0
        if not skipoutput:
            myReport.data.cmsRun1.output.output.files.file0.OutputPFN = self.outpfn

        fwjrDocument = {"_id": "%s-%s" % (jobID, retryCount),
                        "jobid": jobID,
                        "retrycount": retryCount,
                        "fwjr": myReport.__to_json__(None),
                        "type": "fwjr"}
        fwjrdb.queue(fwjrDocument, timestamp = True)
        fwjrdb.commit()


    def _injectJobReport(selfi, couchServer, taskName, jobID):
        jobdb = couchServer.connectDatabase(jsmCacheDB + "/jobs")

        jobDocument = {
            'task': taskName,
            'group': 'Analysis',
            #'name': '13ed3478-abce-11e0-94e6-0026b958c394-4',
            'workflow': taskName.split("/")[1],
            'mask': {'LastRun': None, 'FirstRun': None, 'LastEvent': None, 'FirstEvent': None, 'LastLumi': None, 'FirstLumi': None},
            #'inputfiles': [],
            'jobid': jobID,
            'states': {'0': {'newstate': 'created', 'oldstate': 'new', 'location': 'Agent', 'timestamp': 1310396241}},
            'user': 'mmascher',
            #'jobgroup': 12L,
            'taskType': 'Analysis',
            'owner': 'mmascher',
            '_id': str(jobID),
            'type': 'job'
        }

        jobdb.queue(jobDocument, timestamp = True)
        jobdb.commit()


    def injectLogFWJR(self, jobID, retryCount, taskName):
        """
        Inject a fake fwjr with a logArch1 section into couchDB
        """
        couchServer = CouchServer(os.environ["COUCHURL"])
        fwjrdb = couchServer.connectDatabase(jsmCacheDB + "/fwjrs")

        self._injectJobReport(couchServer, taskName, jobID)

        myReportLog = Report("logArch1")
        myReportLog.setTaskName(taskName)
        myReportLog.addOutputModule(moduleName = "logArchive")
        reportFile = {"lfn": "", "pfn": self.logpfn,
                      "location": "SEName", "module_label": "logArchive",
                      "events": 0, "size": 0, "merged": False}
        myReportLog.addOutputFile(outputModule = "logArchive", file = reportFile)

        fwjrDocumentLog = {"_id": "%s-%s" % (jobID, retryCount),
                        "jobid": jobID,
                        "retrycount": retryCount,
                        "fwjr": myReportLog.__to_json__(None),
                        "type": "fwjr"}
        fwjrdb.queue(fwjrDocumentLog, timestamp = True)
        fwjrdb.commit()


    def testGetJobsFromRange(self):
        """
        Test getJobsFromRange function
        """
        result = getJobsFromRange("1")
        self.assertEqual(result, [1])

        result = getJobsFromRange("1 , 2, 5")
        self.assertEqual(result, [1, 2, 5])

        result = getJobsFromRange("1 , 2-6 , 5 , 7-9")
        self.assertEqual(result, [1, 2, 3, 4, 5, 6, 5, 7, 8, 9])


    def testGetDataLocation(self):
        """
        Test /data API
        """
        api = "data"
        EXP_RES = { 'pfn' : self.outpfn }

        fwjrPath = os.path.join(self.test_data_dir, 'Report.xml')
        self.injectFWJR(fwjrPath, 127, 0, "/%s/Analysis" % self.dataLocParams["requestID"])
        self.injectFWJR(fwjrPath, 128, 0, "/%s/Analysis" % self.dataLocParams["requestID"])

        result, exp = methodTest('GET', self.urlbase + api, self.dataLocParams, \
                        'application/json', 'application/json', {'code' : 200})
        self.assertTrue(exp is not None)
        result = json.loads(result)

        self.assertEqual(result['1'], EXP_RES)
        self.assertEqual(result['2'], EXP_RES)

        #Check job with multiple fwjr
        self.injectFWJR(fwjrPath, 127, 1, "/%s/Analysis" % self.dataLocParams["requestID"])
        result, exp = methodTest('GET', self.urlbase + api, self.dataLocParams, \
                        'application/json', 'application/json', {'code' : 200})
        self.assertTrue(exp is not None)
        result = json.loads(result)

        self.assertEqual(result['1'], EXP_RES)
        self.assertEqual(result['2'], EXP_RES)

        #Test invalid ranges
        self.dataLocParams['jobRange'] = 'I'
        result, exp = methodTest('GET', self.urlbase + api, self.dataLocParams, \
                        'application/json', 'application/json', {'code' : 400})
        self.assertTrue(exp is not None)


    def testGetLogLocation(self):
        """
        Test /log API
        """
        api = "log"
        EXP_RES = { 'pfn' : self.logpfn }

        self.injectLogFWJR(347, 0, "/%s/Analysis" % self.logLocParams["requestID"])
        self.injectLogFWJR(348, 0, "/%s/Analysis" % self.logLocParams["requestID"])

        result, exp = methodTest('GET', self.urlbase + api, self.logLocParams, \
                        'application/json', 'application/json', {'code' : 200})
        result = json.loads(result)
        self.assertEqual(result['1'], EXP_RES)

        #Check job with multiple fwjr
        self.injectLogFWJR(347, 1, "/%s/Analysis" % self.logLocParams["requestID"])
        result, exp = methodTest('GET', self.urlbase + api, self.logLocParams, \
                        'application/json', 'application/json', {'code' : 200})
        self.assertTrue(exp is not None)
        result = json.loads(result)

        self.assertEqual(result['1'], EXP_RES)
        self.assertEqual(result['2'], EXP_RES)

        #Test invalid ranges
        self.logLocParams['jobRange'] = 'I'
        result, exp = methodTest('GET', self.urlbase + api, self.logLocParams, \
                        'application/json', 'application/json', {'code' : 400})
        self.assertTrue(exp is not None)


    def testReport(self):
        """
        Test /report API. Make sure Report.xml generates the same list of processed lumis as what is in JSON fil
        """
        api = "goodLumis"

        print self.test_data_dir
        fwjrPath = os.path.join(self.test_data_dir, 'Report.xml')
        jsonPath = os.path.join(self.test_data_dir, 'reportLumis.json')
        self.injectFWJR(fwjrPath, 127, 0, "/%s/Analysis" % self.reportParams["requestID"])

        result, exp = methodTest('GET', self.urlbase + api, self.reportParams, \
                        'application/json', 'application/json', {'code' : 200})
        self.assertTrue(exp is not None)
        result = json.loads(result)

        with open(jsonPath) as f:
            correctResult = json.load(f)

        self.assertEqual(result, correctResult)


    def testJobErrors(self):
        """
        Test /jobErrors API. Make sure it get back the expected result
        """
        api = 'jobErrors'

        fwjrPath = os.path.join(self.test_data_dir, 'CMSSWFailReport.xml')
        jsonPath = os.path.join(self.test_data_dir, 'reportLumis.json')
        self.injectFWJR(fwjrPath, 127, 0, "/%s/Analysis" % self.reportParams["requestID"], True)

        result, exp = methodTest('GET', self.urlbase + api, self.reportParams, \
                        'application/json', 'application/json', {'code' : 200})

        self.assertTrue(exp is not None)
        result = json.loads(result)

        self.assertEqual(len(result.keys()), 1)
        self.assertEqual(result.keys(), [u'1'])
        self.assertEqual(result[u'1'][u'0'][u'cmsRun1'][0][u'type'], u'CMSException')
        self.assertEqual(result[u'1'][u'0'][u'cmsRun1'][0][u'exitCode'], u'8001')


    def testUpload(self):
        """
        Test uploading with curl
        Make sure size returned by server is what we expect
        """

        testInputName  = '/tmp/UnitTestInputFile'
        try:
            os.unlink(testInputName)
        except OSError:
            pass

        with open(testInputName, 'w') as testFile:
            testFile.write(str(time.time()))
            testFile.write('\nFirst line\n')
            [testFile.write(str(x)) for x in xrange(0,1000)]
            testFile.write('\nLast line\n')

        sha256sum = hashlib.sha256()
        with open(testInputName,'rb') as f:
            while True:
                chunkdata = f.read(8192)
                if not chunkdata:
                    break
                sha256sum.update(chunkdata)

        with tempfile.NamedTemporaryFile() as tmpFile:
            url = self.urlbase + 'uploadUserSandbox'
            curlCommand = 'curl -H "Accept: application/json" -F "checksum=%s" -F "doUpload=%s" -F"userfile=@%s" %s -o %s' % \
                          (sha256sum.hexdigest(), doUpload, testInputName, url, tmpFile.name)
            (status, output) = commands.getstatusoutput(curlCommand)
            self.assertEqual(status, 0, 'Upload failed with output %s' % output)
            returnDict = json.loads(tmpFile.read())

            self.assertEqual(returnDict['size'], os.path.getsize(testInputName))

        return returnDict


if __name__ == "__main__":
    unittest.main()
