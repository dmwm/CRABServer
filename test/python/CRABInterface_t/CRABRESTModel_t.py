#!/usr/bin/env/ python
#pylint: disable=E1101
"""
_CRABRESTModel_t_

"""

import commands
import hashlib
import json
import unittest
import logging
import os
import tarfile
import tempfile
import time
import urllib2

from WMQuality.WebTools.RESTClientAPI import methodTest
from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTServerSetup import DefaultConfig
from WMCore.Cache.WMConfigCache import ConfigCache
from WMCore.Database.CMSCouch import CouchServer
from WMCore.FwkJobReport.Report import Report
try:
    from CRABServer.CRABInterface.CRABRESTModel import expandRange
except ImportError:
    from CRABInterface.CRABRESTModel import expandRange
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import allSoftwareVersions
import WMCore.RequestManager.RequestDB.Interface.Admin.SoftwareManagement as SoftwareAdmin
from WMCore.RequestManager.RequestDB.Interface.Request import ChangeState


databaseURL = os.getenv("DATABASE")
databaseSocket = os.getenv("DBSOCK")

couchURL = os.getenv("COUCHURL")
workloadDB = 'workload_db_test'
configCacheDB = 'config_cache_test'
ACDCDB = 'acdc_test'
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
        "PublishDataName": "MyTest",
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

    lumiMaskParams = {'DatasetName' : "/RelValProdTTbar/JobRobot-MC_3XY_V24_JobRobot-v1/GEN-SIM-DIGI-RECO",
                      'Description' : '',
                      'RequestName' : "crab_MyAnalysis__",
                      "Group"       : "Analysis",
                      "UserDN"      : "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=mmascher/CN=720897/CN=Marco Mascheroni",
                      'Label'       : '',
                     }

    dataLocParams = {
#        "requestName" : "mmascher_crab_MyAnalysis___110429_030846",
        "jobRange" : '1-2'
    }

    logLocParams = {
#        "requestName" : "mmascher_crab_MyAnalysis___110506_123756",
        "jobRange" : '1-2'
    }

    reportParams = {
        "requestName" : "mmascher_crab_MyAnalysis___110506_123756",
    }

    statusParams = {
        "requestName" : "mmascher_crab_MyAnalysis___110506_123756",
    }

    location = 'T2_BE_UCL'
    lfnAsync = '/store/user/mmascher/RelValProdTTbar/1304039730//0000/outputAsync.root'
    outpfn = 'srm://ingrid-se02.cism.ucl.ac.be:8444/srm/managerv2?SFN=/storage/data/cms/store/user/mmascher/RelValProdTTbar/1304039730/' + \
             '/0000/output.root'
    outpfnAsync = 'srm://ingrid-se02.cism.ucl.ac.be:8444/srm/managerv2?SFN=/storage/data/cms/store/user/mmascher/RelValProdTTbar/1304039730/' + \
             '/0000/outputAsync.root'
    logLocation = 'T2_IT_Bari'
    loglfn = '/store/unmerged/logs/prod/2011/5/6/mmascher_crab_MyAnalysis___110506_123756/Analysis/0000/0/f56e599e-77cc-11e0-b51e-0026b958c394-99-0-logArchive.tar.gz'
    logpfn = 'srm://storm-se-01.ba.infn.it:8444/srm/managerv2?SFN=//cms/store/unmerged/logs/prod/2011/5/6/' + \
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
        self.config.UnitTests.views.active.rest.model.object = 'CRABInterface.CRABRESTModel'
        self.config.UnitTests.views.active.rest.model.couchUrl = couchURL
        self.config.UnitTests.views.active.rest.model.workloadCouchDB = workloadDB
        self.config.UnitTests.views.active.rest.configCacheCouchURL = couchURL
        self.config.UnitTests.views.active.rest.configCacheCouchDB = configCacheDB
        self.config.UnitTests.views.active.rest.ACDCCouchURL = couchURL
        self.config.UnitTests.views.active.rest.ACDCCouchDB = ACDCDB
        self.config.UnitTests.views.active.rest.jsmCacheCouchURL = couchURL
        self.config.UnitTests.views.active.rest.jsmCacheCouchDB = jsmCacheDB
        self.config.UnitTests.views.active.rest.DBSUrl = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
        self.config.UnitTests.views.active.rest.serverDN = ''
        self.config.UnitTests.views.active.rest.sandBoxCacheHost = 'cms-xen39.fnal.gov'
        self.config.UnitTests.views.active.rest.sandBoxCachePort = 7739
        self.config.UnitTests.views.active.rest.sandBoxCacheBasepath = 'userfilecache/userfilecache'
        self.config.UnitTests.views.active.rest.logLevel = 'DEBUG'

        self.schemaModules = ['WMCore.RequestManager.RequestDB']
        self.urlbase = self.config.getServerUrl()


    #Override setup to add software versions
    def setUp(self):
        """
        _setUp_
        """
        data_path = os.path.join(os.path.dirname(__file__), '../../data')
        self.test_data_dir = os.path.normpath(data_path)

        RESTBaseUnitTest.setUp(self)
        self.testInit.setupCouch(workloadDB, 'ReqMgr')
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


    def postRequest(self, request, inputparams, code):
        """
        _postRequest_
        """
        api = "task/%s" % request

        jsonString = json.dumps(inputparams, sort_keys=False)
        result, exp = methodTest('POST', self.urlbase + api, jsonString, 'application/json', \
                                 'application/json', {'code' : code})
        self.assertTrue(exp is not None)
        return json.loads(result)


    def testPostRequest(self):
        """
        _testPostRequest_
        """

        #_insertConfig has been already tested in the previous test method
        result = self.insertConfig()
        localPostReqParams = self.postReqParams.copy()
        localPostReqParams['AnalysisConfigCacheDoc'] = result['DocID']

        #Posting a request without registering the user first
        result = self.postRequest( localPostReqParams['RequestName'], localPostReqParams, 500)

        #Again, insertUser tested before. It should work
        self.insertUser()
        result = self.postRequest( localPostReqParams['RequestName'], localPostReqParams, 200)
        self.assertTrue(result.has_key("ID"))
        #SINCE python 2.7 :(
        #self.assertRegexpMatches(result['ID'], \
        #           "mmascher_crab_MyAnalysis__\d\d\d\d\d\d_\d\d\d\d\d\d")
        self.assertTrue(result['ID'].startswith("%s_%s" % (localPostReqParams["Username"], \
                                          localPostReqParams["RequestName"])))
        self.assertEqual(result['ProcessingVersion'], 'v2')

        localPostReqParams['SiteWhitelist'] = ['XXX']
        result = self.postRequest( localPostReqParams['RequestName'] + '1', localPostReqParams, 400)
        self.assertTrue(result['message'].find('provided in the SiteWhitelist param has not been found.') > 1)

        localPostReqParams['SiteWhitelist'] = ['T2*']
        result = self.postRequest( localPostReqParams['RequestName'] + '1', localPostReqParams, 200)
        couchDoc = self.testInit.couch.couchServer.connectDatabase(workloadDB).document(result['ID'])
        self.assertTrue(len(couchDoc['SiteWhitelist']) > 10) #I assume there are more than 10 T2
        del localPostReqParams['SiteWhitelist']

        localPostReqParams['RunWhitelist'] = '1,2.4'
        result = self.postRequest( localPostReqParams['RequestName'] + '2', localPostReqParams, 400)
        self.assertEqual(result['message'], 'Irregular range 1,2.4')

        localPostReqParams['RunWhitelist'] = '1,2-4'
        result = self.postRequest( localPostReqParams['RequestName'] + '3', localPostReqParams, 200)
        self.assertTrue(result.has_key("ID"))

        # Test inserting a workflow with a new processing version
        localPostReqParams['ProcessingVersion'] = 'v10'
        result = self.postRequest( localPostReqParams['RequestName'] + '4', localPostReqParams, 200)
        self.assertEqual(result['ProcessingVersion'], 'v10')
        localPostReqParams['ProcessingVersion'] = ''

        # Test various problems with asyncDest
        localPostReqParams['asyncDest'] = 'T2_US_Bari'
        result = self.postRequest( localPostReqParams['RequestName'] + '5', localPostReqParams, 400)
        self.assertTrue(result['message'].find('not a valid CMS site') > 1)

        localPostReqParams['asyncDest'] = 'Bari'
        result = self.postRequest( localPostReqParams['RequestName'] + '6', localPostReqParams, 400)
        self.assertTrue(result['message'].find('not a valid CMS site name') > 1)

        del localPostReqParams['asyncDest']
        result = self.postRequest( localPostReqParams['RequestName'] + '7', localPostReqParams, 400)
        self.assertTrue(result['message'] == 'asyncDest parameter is missing from request')


    def resubmit(self, request, inputparams, code):
        """
        _resubmit_
        """
        api = "reprocessTask/%s" % request

        jsonString = json.dumps(inputparams, sort_keys=False)
        result, exp = methodTest('POST', self.urlbase + api, jsonString, 'application/json', \
                                 'application/json', {'code' : code})
        self.assertTrue(exp is not None)
        return json.loads(result)


    def testResubmit(self):
        """
        _testRequest_
        """

        ## testing without the original request
        result = self.resubmit( 'daffy_duck', {}, 400 )
        self.assertTrue(result['message'] == 'Request daffy_duck not found. Impossible to resubmit.')

        ## creating a valid workflow to be resubmitted
        resultConfig = self.insertConfig()
        self.postReqParams['AnalysisConfigCacheDoc'] = resultConfig['DocID']
        self.insertUser()
        resultSub = self.postRequest( self.postReqParams['RequestName'], self.postReqParams, 200)

        ## testing with the original request in place, but not completed
        resultResub = self.resubmit( resultSub['ID'], {}, 400 )
        self.assertTrue(resultResub['message'].find('not yet completed; impossible to resubmit') > 1)

        ##### disabling this till we have a easy way to have a real spec, or emulate one #####
        ## testing with the original request in place, but not completed, this time we force it
        #resultResub = self.resubmit( resultSub['ID'], {"ForceResubmit": 1}, 200 )
        #assert type(result.get('ID', '')) is type('string')
        #assert len(result.get('ID', '')) > 0

        ## creating a new valid request to be resubmitted
        newsub = self.postReqParams.copy()
        newsub['RequestName'] = 'mickey_mouse'
        resultSub = self.postRequest( newsub['RequestName'], newsub, 200)
        ## testing with the original request in place, completed
        ChangeState.changeRequestStatus(resultSub['ID'], 'completed')
        ##### disabling this till we have a easy way to have a real spec, or emulate one #####
        #resultResub = self.resubmit( resultSub['ID'], {}, 200 )
        #assert type(result.get('ID', '')) is type('string')
        #assert len(result.get('ID', '')) > 0

        ## creating a new valid request to be resubmitted
        newsub = self.postReqParams.copy()
        newsub['RequestName'] = 'donald_duck'
        resultSub = self.postRequest( newsub['RequestName'], newsub, 200)
        ## testing with the original request in place, completed
        ChangeState.changeRequestStatus(resultSub['ID'], 'completed')
        ##### disabling this till we have a easy way to have a real spec, or emulate one #####
        #resultResub = self.resubmit( resultSub['ID'], {}, 200 )
        #assert type(result.get('ID', '')) is type('string')
        #assert len(result.get('ID', '')) > 0

        ## creating a new valid request to be resubmitted
        newsub = self.postReqParams.copy()
        newsub['RequestName'] = 'plut'
        resultSub = self.postRequest( newsub['RequestName'], newsub, 200)
        ## testing with the original request in place, completed and with a white-blackList
        ChangeState.changeRequestStatus(resultSub['ID'], 'completed')
        ##### disabling this till we have a easy way to have a real spec, or emulate one #####
        #resultResub = self.resubmit( resultSub['ID'], {'SiteWhitelist': 'T2_IT_Legnaro', 'SiteBlacklist': 'T2_US_UCSD'}, 200 )
        #assert type(result.get('ID', '')) is type('string')
        #assert len(result.get('ID', '')) > 0


    def injectFWJR(self, reportXML, jobID, retryCount, taskName, skipoutput = False, addAsyncStep = False):
        """
        Inject a fake fwjr with a cmsRun1 section into couchDB
        """
        couchServer = CouchServer(os.environ["COUCHURL"])

        self._injectJobReport(couchServer, taskName, jobID)

        fwjrdb = couchServer.connectDatabase(jsmCacheDB + "/fwjrs")

        myReport = Report("cmsRun1")
        myReport.parse(reportXML)
        myReport.setTaskName(taskName)

        #adding async step
        if addAsyncStep:
            myReport.addOutputModule(moduleName = "asynStageOut")
            myReport.addStep("asyncStageOut1", 0)
            reportFile = {"OutputPFN": self.outpfnAsync, "lfn" : self.lfnAsync, "location" : self.location}
            myReport.addOutputFile(outputModule = "output", file = reportFile)

        myReport.data.cmsRun1.status = 0
        if not skipoutput:
            myReport.data.cmsRun1.output.output.files.file0.OutputPFN = self.outpfn
            myReport.data.cmsRun1.output.output.files.file0.location = self.location

        fwjrDocument = {"_id": "%s-%s" % (jobID, retryCount),
                        "jobid": jobID,
                        "retrycount": retryCount,
                        "fwjr": myReport.__to_json__(None),
                        "type": "fwjr"}
        fwjrdb.queue(fwjrDocument, timestamp = True)
        fwjrdb.commit()


    def _injectJobReport(self, couchServer, taskName, jobID):
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
        reportFile = {"lfn": self.loglfn, "pfn": self.logpfn,
                      "location": self.logLocation, "module_label": "logArchive",
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
        Test expandRange function
        """
        result = expandRange("1")
        self.assertEqual(result, [1])

        result = expandRange("1 , 2, 5")
        self.assertEqual(result, [1, 2, 5])

        result = expandRange("1 , 2-6 , 5 , 7-9")
        self.assertEqual(result, [1, 2, 3, 4, 5, 6, 5, 7, 8, 9])


    def testGetDataLocation(self):
        """
        Test /data API
        """
        api = "data/"
        EXP_RES = { 'pfn' : self.outpfn }
        fwjrPath = os.path.join(self.test_data_dir, 'Report.xml')

        resultConfig = self.insertConfig()
        self.postReqParams['AnalysisConfigCacheDoc'] = resultConfig['DocID']
        self.insertUser()
        resultSub = self.postRequest( self.postReqParams['RequestName'], self.postReqParams, 200)
        self.injectFWJR(fwjrPath, 127, 0, "/%s/Analysis" % resultSub['ID'])
        self.injectFWJR(fwjrPath, 128, 0, "/%s/Analysis" % resultSub['ID'])
        #self.dataLocParams['requestName'] = resultSub['ID']
        result, exp = methodTest('GET', self.urlbase + api + resultSub['ID'], self.dataLocParams, \
                        'application/json', 'application/json', {'code' : 200})
        self.assertTrue(exp is not None)
        result = json.loads(result)
        self.assertEqual(result['data'][0]['output']['1'], EXP_RES)
        self.assertEqual(result['data'][0]['output']['2'], EXP_RES)

        #Check job with multiple fwjr
        self.injectFWJR(fwjrPath, 127, 1, "/%s/Analysis" % resultSub['ID'])
        result, exp = methodTest('GET', self.urlbase + api + resultSub['ID'], self.dataLocParams, \
                        'application/json', 'application/json', {'code' : 200})
        self.assertTrue(exp is not None)
        result = json.loads(result)

        self.assertEqual(result['data'][0]['output']['1'], EXP_RES)
        self.assertEqual(result['data'][0]['output']['2'], EXP_RES)

        #Async step is present this time
        self.injectFWJR(fwjrPath, 127, 2, "/%s/Analysis" % resultSub['ID'], addAsyncStep = True)
        result, exp = methodTest('GET', self.urlbase + api + resultSub['ID'], self.dataLocParams, \
                        'application/json', 'application/json', {'code' : 200})
        self.assertTrue(exp is not None)
        result = json.loads(result)
        self.assertEqual(result['data'][0]['output']['1'], { 'pfn' : self.outpfnAsync})

        #Test invalid ranges
        self.dataLocParams['jobRange'] = 'I'
        result, exp = methodTest('GET', self.urlbase + api + resultSub['ID'], self.dataLocParams, \
                        'application/json', 'application/json', {'code' : 400})
        self.assertTrue(exp is not None)


    def testGetLogLocation(self):
        """
        Test /log API
        """
        api = "log/"
        EXP_RES = { 'pfn' : self.logpfn }

        resultConfig = self.insertConfig()
        self.postReqParams['AnalysisConfigCacheDoc'] = resultConfig['DocID']
        self.insertUser()
        resultSub = self.postRequest( self.postReqParams['RequestName'], self.postReqParams, 200)
        self.injectLogFWJR(347, 0, "/%s/Analysis" % resultSub['ID'])
        self.injectLogFWJR(348, 0, "/%s/Analysis" % resultSub['ID'])

        result, exp = methodTest('GET', self.urlbase + api + resultSub['ID'], self.logLocParams, \
                        'application/json', 'application/json', {'code' : 200})
        result = json.loads(result)
        self.assertEqual(result['log'][0]['output']['1'], EXP_RES)

        #Check job with multiple fwjr
        self.injectLogFWJR(347, 1, "/%s/Analysis" % resultSub['ID'])
        result, exp = methodTest('GET', self.urlbase + api + resultSub['ID'], self.logLocParams, \
                        'application/json', 'application/json', {'code' : 200})
        self.assertTrue(exp is not None)
        result = json.loads(result)

        self.assertEqual(result['log'][0]['output']['1'], EXP_RES)
        self.assertEqual(result['log'][0]['output']['2'], EXP_RES)

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

        # insertConfig has been already tested in the previous test method
        resultuser = self.insertConfig()
        localPostReqParams = self.postReqParams.copy()
        localPostReqParams['AnalysisConfigCacheDoc'] = resultuser['DocID']

        # insertUser tested before. It should work
        self.insertUser()

        # postRequest tested before.
        resultsub = self.postRequest( localPostReqParams['RequestName'], localPostReqParams, 200)

        fwjrPath = os.path.join(self.test_data_dir, 'Report.xml')
        jsonPath = os.path.join(self.test_data_dir, 'reportLumis.json')
        self.injectFWJR(fwjrPath, 127, 0, "/%s/Analysis" % resultsub["ID"])

        reportPar = {"requestName" : resultsub["ID"]}

        result, exp = methodTest('GET', self.urlbase + api, reportPar, \
                        '*/*', 'application/json', {'code' : 200})
        self.assertTrue(exp is not None)

        result = eval(result)
        with open(jsonPath) as f:
            correctResult = json.load(f)
        self.assertEqual(eval(result['lumis'][0]['lumis'])["1"], correctResult["1"])


    def testJobErrors(self):
        """
        Test /jobErrors API. Make sure it get back the expected result
        """
        api = 'jobErrors'

        # insertConfig has been already tested in the previous test method
        resultuser = self.insertConfig()
        localPostReqParams = self.postReqParams.copy()
        localPostReqParams['AnalysisConfigCacheDoc'] = resultuser['DocID']

        # insertUser tested before. It should work
        self.insertUser()

        # postRequest tested before.
        resultsub = self.postRequest( localPostReqParams['RequestName'], localPostReqParams, 200)

        fwjrPath = os.path.join(self.test_data_dir, 'CMSSWFailReport.xml')
        jsonPath = os.path.join(self.test_data_dir, 'reportLumis.json')
        self.injectFWJR(fwjrPath, 127, 0, "/%s/Analysis" % resultsub["ID"], True)

        result, exp = methodTest('GET', self.urlbase + api, {'requestName': resultsub["ID"]}, \
                        'application/json', 'application/json', {'code' : 200})

        self.assertTrue(exp is not None)
        result = json.loads(result)

        self.assertEqual(len(result["errors"]), 1)
        self.assertEqual(result["errors"][0]["details"].keys(), [u'1'])
        self.assertEqual(result["errors"][0]["details"][u'1'][u'0'][u'cmsRun1'][0][u'type'], u'CMSException')
        self.assertEqual(result["errors"][0]["details"][u'1'][u'0'][u'cmsRun1'][0][u'exitCode'], u'8001')


    def testStatus(self):
        """
        Test detailed status
        """
        api = "task"

        # insertConfig has been already tested in the previous test method
        resultuser = self.insertConfig()
        localPostReqParams = self.postReqParams.copy()
        localPostReqParams['AnalysisConfigCacheDoc'] = resultuser['DocID']

        # insertUser tested before. It should work
        self.insertUser()

        # postRequest tested before.
        resultsub = self.postRequest( localPostReqParams['RequestName'], localPostReqParams, 200)

        taskn = "/%s/Analysis" % resultsub["ID"]
        fwjrPath = os.path.join(self.test_data_dir, 'Report.xml')
        self.injectFWJR(fwjrPath, 127, 0, taskn, skipoutput=True)
        fwjrPath = os.path.join(self.test_data_dir, 'CMSSWFailReport.xml')
        self.injectFWJR(fwjrPath, 128, 0, taskn, skipoutput=True)

        statusPar = {"requestName": resultsub["ID"]}

        result, exp = methodTest('GET', self.urlbase + api, statusPar, \
                        'application/json', 'application/json', {'code' : 200})
        self.assertTrue(exp is not None)
        result = json.loads(result)
        self.assertEqual(result[u'workflows'][0]['states'][taskn]['pending']['count'], 2)
        self.assertEqual(sorted(result[u'workflows'][0]['states'][taskn]['pending']['jobs']), [1, 2])
        self.assertEqual(sorted(result[u'workflows'][0]['states'][taskn]['pending']['jobIDs']), [127, 128])


    def testUpload(self):
        """
        Test uploading with curl
        Make sure size returned by server is what we expect
        """

        testInputName  = '/tmp/UnitTestInputFile'
        testTarName    = '/tmp/test.tgz'
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

        tar = tarfile.open(name=testTarName, mode='r:gz')
        lsl = [(x.name, int(x.size), int(x.mtime), x.uname) for x in tar.getmembers()]
        hasher = hashlib.sha256(str(lsl))

        with tempfile.NamedTemporaryFile() as tmpFile:
            url = self.urlbase + 'uploadUserSandbox'
            curlCommand = 'curl -H "Accept: application/json" -F "checksum=%s" -F "doUpload=%s" -F"userfile=@%s" %s -o %s' % \
                          (hasher.hexdigest(), doUpload, testTarName, url, tmpFile.name)
            (status, output) = commands.getstatusoutput(curlCommand)
            self.assertEqual(status, 0, 'Upload failed with output %s' % output)
            returnDict = json.loads(tmpFile.read())

            self.assertEqual(returnDict['size'], os.path.getsize(testTarName))

        return returnDict


    def testACDCUpload(self):
        """
        Test upload the lumi mask to the server
        """
        api = 'lumiMask'
        lumiFile = urllib2.urlopen('https://cms-service-dqm.web.cern.ch/cms-service-dqm/CAF/certification/Collisions10/7TeV/DCSOnly/DCSTRONLY_132440-139459')
        lumiMask = json.load(lumiFile)

        self.lumiMaskParams.update({'LumiMask' : lumiMask})

        jsonString = json.dumps(self.lumiMaskParams, sort_keys=False)
        resultString, exp = methodTest('POST', self.urlbase + api, jsonString, 'application/json', \
                                       'application/json', {'code' : 200})
        result = json.loads(resultString)

        self.assertTrue(result.has_key("DocID"))
        self.assertTrue(result.has_key("DocRev"))
        self.assertTrue(result.has_key("Name"))
        self.assertTrue(len(result["DocID"]) > 0)
        self.assertTrue(len(result["DocRev"]) > 0)
        self.assertTrue(len(result["Name"]) > 0)

        return


    def testDeleteRequest(self):
        """
        Test the deletion of a request
        """

        def kill(request, code):
            """
            _resubmit_
            """
            api = "task/%s" % request

            jsonString = json.dumps({}, sort_keys=False)
            result, exp = methodTest('DELETE', self.urlbase + api, jsonString, 'application/json', \
                                     'application/json', {'code' : code})
            self.assertTrue(exp is not None)
            return json.loads(result)

        ## killing a not existing wf
        result = kill('donald_duck', 400)
        self.assertTrue(result["message"] == "Cannot find request donald_duck")

        ## creating a valid workflow to be killed
        resultConfig = self.insertConfig()
        self.postReqParams['AnalysisConfigCacheDoc'] = resultConfig['DocID']
        self.insertUser()
        resultSub = self.postRequest( self.postReqParams['RequestName'], self.postReqParams, 200)

        ## killing the wf, this should kill it without any issue
        result = kill(resultSub['ID'], 500)
        # this is currently failing, probably because of the encapsuled test environment
        # beacause reqmgr_assigned_prodmgr table in req-mgr is empty
        #self.assertTrue(result["result"] == "ok")

        ## killing again the wf, this should fail
        result = kill(resultSub['ID'], 500)
        self.assertTrue(result["message"].find("impossible to kill") > -1)


if __name__ == "__main__":
    unittest.main()
