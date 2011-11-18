"""
CRAB Interface to the WMAgent
"""

import cherrypy
import commands
import copy
import hashlib
import shutil
import tarfile
import tempfile
import threading
import time
import json
import imp
from operator import itemgetter

from WMCore.ACDC.AnalysisCollectionService import AnalysisCollectionService
from WMCore.WebTools.RESTModel import restexpose
from WMCore.WebTools.RESTModel import RESTModel
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import unidecode, removePasswordFromUrl
from WMCore.RequestManager.RequestMaker.Registry import retrieveRequestMaker
from WMCore.Database.CMSCouch import CouchServer, CouchError
from WMCore.DataStructs.Mask import Mask
import WMCore.RequestManager.RequestDB.Connection as DBConnect
import WMCore.Wrappers.JsonWrapper as JsonWrapper
import WMCore.Lexicon
from WMCore.RequestManager.RequestMaker import CheckIn
import WMCore.RequestManager.RequestMaker.Processing.AnalysisRequest
from WMCore.RequestManager.RequestDB.Interface.Request import ChangeState, GetRequest
from WMCore.RequestManager.RequestDB.Interface.User import Registration
from WMCore.RequestManager.RequestDB.Interface.Group import Information
from WMCore.RequestManager.RequestDB.Interface.Admin import GroupManagement, ProdManagement
from WMCore.Cache.WMConfigCache import ConfigCache, ConfigCacheException
from WMCore.Services.Requests import JSONRequests
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import loadWorkload
import WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools as Utilities

from LFN2PFNConverter import LFN2PFNConverter


def getJobsFromRange(myrange):
    """
    Take a string and return a list of jobId
    """
    myrange = myrange.replace(' ','').split(',')
    result = []
    for element in myrange:
        if element.count('-') > 0:
            mySubRange = element.split('-')
            jobInterval = range( int(mySubRange[0]), int(mySubRange[1])+1)
            result.extend(jobInterval)
        else:
            result.append(int(element))

    return result

class CRABRESTModel(RESTModel):
    """ CRAB Interface to the WMAgent """

    def __init__(self, config={}):
        '''
        Initialise the RESTModel and add some methods to it.
        '''
        RESTModel.__init__(self, config)

        self.couchUrl = config.model.couchUrl
        self.workloadCouchDB = config.model.workloadCouchDB
        self.ACDCCouchURL = config.ACDCCouchURL
        self.ACDCCouchDB = config.ACDCCouchDB
        self.DBSURL = config.DBSUrl

        self.configCacheCouchURL = config.configCacheCouchURL
        self.configCacheCouchDB = config.configCacheCouchDB
        self.jsmCacheCouchURL = config.jsmCacheCouchURL
        self.jsmCacheCouchDB = config.jsmCacheCouchDB
        self.serverDN = config.serverDN
        self.sandBoxCacheHost = config.sandBoxCacheHost
        self.sandBoxCachePort = config.sandBoxCachePort
        self.sandBoxCacheBasepath = config.sandBoxCacheBasepath

        self.converter = LFN2PFNConverter()

        self.clientMapping = {}
        if hasattr(config, 'clientMapping'):
            clientmapper = imp.load_source('', config.clientMapping)
            self.clientMapping = clientmapper.defaulturi

        #/user
        self._addMethod('POST', 'user', self.addNewUser,
                        args=[],
                        validation=[self.isalnum])
        #/task
        self._addMethod('GET', 'task', self.getDetailedStatus,
                        args=['requestName'],
                        validation=[self.isalnum])
        self._addMethod('PUT', 'task', self.putTaskModifies,
                        args=['requestName'],
                        validation=[self.isalnum])
        self._addMethod('DELETE', 'task', self.deleteRequest,
                        args=['requestName'],
                        validation=[self.isalnum])
        self._addMethod('POST', 'task', self.postRequest,
                        args=['requestName'],
                        validation=[self.isalnum])
        #/reprocessTask
        self._addMethod('POST', 'reprocessTask', self.reprocessRequest,
                        args=['requestName'],
                        validation=[self.isalnum])
        #/config
        self._addMethod('POST', 'config', self.postUserConfig,
                        args=[],
                        validation=[self.checkConfig])
        #/data
        self._addMethod('GET', 'data', self.getDataLocation,
                       args=['requestName','jobRange'])

        #/goodLumis, equivalent of "report" from CRAB2
        self._addMethod('GET', 'goodLumis', self.getGoodLumis,
                       args=['requestName'], validation=[self.checkConfig])

        #/lumiMask
        self._addMethod('POST', 'lumiMask', self.postLumiMask,
                       args=[], validation=[self.checkConfig])

        #/log
        self._addMethod('GET', 'log', self.getLogLocation,
                       args=['requestName','jobRange'], validation=[self.checkConfig])

        # Server
        self._addMethod('GET', 'info', self.getServerInfo,
                        args=[],
                        validation=[self.isalnum])

        self._addMethod('GET', 'requestmapping', self.getClientMapping,
                        args=[],
                        validation=[self.isalnum])

        # this allows to retrieve failure reason for each job
        self._addMethod('GET', 'jobErrors', self.getJobErrors,
                        args=['requestName'],
                        validation=[self.isalnum])

        # uploadConfig. Add directly since the file cannot be parsed through validation
        self.methods['POST']['uploadUserSandbox'] = {'args':       ['userfile', 'checksum', 'doUpload'],
                                          'call':       self.uploadUserSandbox,
                                          'validation': [],
                                          'version':    1,
                                          'expires':    self.defaultExpires}

        cherrypy.engine.subscribe('start_thread', self.initThread)


    def initThread(self, thread_index):
        """
        The ReqMgr expects the DBI to be contained in the Thread
        """
        myThread = threading.currentThread()
        #myThread = cherrypy.thread_data
        # Get it from the DBFormatter superclass
        myThread.dbi = self.dbi

    def postError(self, errmsg, dbgmsg, code):
        """
        Common method to return and log errors
        """
        self.logger.error(errmsg)
        self.logger.debug(dbgmsg)
        raise cherrypy.HTTPError(code, errmsg)

    def __getFromCampaign(self, requestName):
        """
        Receive a string correspondi to a request name
        Return all the workflow grouped by the campaign of the input workflow
        """
        factory = DBConnect.getConnection()
        idDAO = factory(classname = "Request.ID")
        requestId = idDAO.execute(requestName)
        campaignDAO = factory(classname = "Campaign.GetByRequest")
        campaign = campaignDAO.execute(requestId)

        self.database = None
        try:
            self.logger.debug("Connecting to database %s using the couch instance at %s: " % (self.workloadCouchDB, self.couchUrl))
            self.couchdb = CouchServer(self.couchUrl)
            self.database = self.couchdb.connectDatabase(self.workloadCouchDB)
        except CouchError, ex:
            self.postError("Error connecting to couch database", str(ex) + '\n' + str(getattr(ex, 'reason', '')), 500)
        options = {"startkey": [campaign], "endkey": [campaign], 'reduce' : False }
        campaignWfs = self.database.loadView("ReqMgr", "requestsByCampaign", options)
        self.logger.debug("Found %d rows in the reqmgr database." % len(campaignWfs["rows"]))
        return campaignWfs["rows"]

    def checkConfig(self, pset):
        """
        Check user configuration
        """
        return pset

    def postUserConfig(self):
        """
        Act as a proxy for CouchDB. Upload user config and return DocID.
        """

        body = cherrypy.request.body.read()
        params = {}
        ## is this try except really needed? the proper checks should be already performed when this method has been called
        ## we'll need to have specific exception handling
        try:
            params = unidecode(JsonWrapper.loads(body))
        except Exception, ex:
            msg = "Error: problem decoding the body of the request"
            self.postError(msg, str(body), 400)

        ## we'll need to have specific exception handling
        try:
            user = SiteDBJSON().dnUserName(params['UserDN'])
        except Exception, ex:
            self.postError("Problem extracting user from SiteDB", str(ex) + " " + str(params), 500)

        result = {}
        try:
            configCache = ConfigCache(self.configCacheCouchURL, self.configCacheCouchDB)

            configCache.createUserGroup(params['Group'], user)

            configMD5 = hashlib.md5(params['ConfFile']).hexdigest()
            configCache.document['md5_hash'] = configMD5
            configCache.document['pset_hash'] = params['PsetHash']
            configCache.attachments['configFile'] = params['ConfFile']

            configCache.setPSetTweaks(json.loads(params['PsetTweaks']))

            configCache.setLabel(params['Label'])
            configCache.setDescription(params['Description'])
            configCache.save()
            result['DocID']  = configCache.document["_id"]
            result['DocRev'] = configCache.document["_rev"]
        except ConfigCacheException, ex:
            msg = "Error: problem uploading the configuration"
            self.postError(msg, 'Exception: ' + str(ex) + '\nParams: ' + str(params), 500)

        return result

    def postLumiMask(self):
        """
        Act as a proxy for CouchDB. Upload ACDC collection and return DocID.
        """
        self.logger.info('User is uploading lumi mask')
        body = cherrypy.request.body.read()
        params = unidecode(JsonWrapper.loads(body))

        lumiMask = Mask()

        for run, lumis in params['LumiMask'].items():
            lumiMask.addRunWithLumiRanges(run=int(run), lumiList=lumis)

        try:
            user = SiteDBJSON().dnUserName(params['UserDN'])
        except Exception, ex:
            raise cherrypy.HTTPError(500, "Problem extracting user from SiteDB %s" % str(ex))

        acService = AnalysisCollectionService(url=self.ACDCCouchURL, database=self.ACDCCouchDB)
        collection = acService.createCollection(params['RequestName'], user, params['Group'])
        fileSetName = "%s-cmsRun1" % params['RequestName']
        dbsURL = params.get('DbsUrl', self.DBSURL)

        fileSet, fileList = acService.createFilesetFromDBS(
                                collection, filesetName=fileSetName, dbsURL=dbsURL,
                                dataset=params['DatasetName'], mask=lumiMask
                            )

        result = {}
        result['DocID']  = fileList["_id"]
        result['DocRev'] = fileList["_rev"]
        result['Name']   = fileSetName

        return result

    def getServerInfo(self):
        """
        Return informatiion to allow client operations
        """

        result = {}
        ## could be a list of all supported DN
        result['server_dn']  = self.serverDN
        result['my_proxy'] = 'myproxy.cern.ch'

        return result

    def isalnum(self, call_input):
        """
        Validates that all input is alphanumeric, with spaces and underscores
        tolerated.
        """
        for v in call_input.values():
            WMCore.Lexicon.identifier(v)
        return call_input

    def putTaskModifies(self, requestName):
        """
        Modify the task in any possible field : B/W lists, stop/start automation...
        """
        self.postError("Not implemented", '', 501)
        return requestName

    def deleteRequest(self, requestName):
        """
        Delete a request identified by requestName
        """
        try:
            WMCore.Lexicon.requestName(requestName)
        except AssertionError, ex:
            self.postError("Invalid request name specified", '', 400)

        campaignWfs = [(singleWf['value']['Submission'], singleWf['value']['RequestName']) for singleWf in self.__getFromCampaign(requestName)]
        if not campaignWfs:
            self.postError("Cannot find request %s" % requestName, '', 400)
        lastSubmission = max(campaignWfs)[1]

        ## We need to check the status of the request and determine the aborted/failed status based on the current status
        skipDelete = ["aborted", "failed", "completed"]

        requestDetails = {'RequestStatus': 'unknown'}
        try:
            self.logger.info("Going to kill now")
            requestDetails = GetRequest.getRequestDetails(lastSubmission)
        except RuntimeError, re:
            import traceback
            self.postError(str(re), str(traceback.format_exc()), 500)

        ## cannot delete status unknown
        if requestDetails['RequestStatus'] == 'unknown':
            self.postError('Request unknown, impossible to kill' % str(requestDetails['RequestStatus']), '', 500)
        ## cannot delete when in some terminal status
        elif requestDetails['RequestStatus'] in skipDelete:
            self.postError('Status of the request is %s: impossible to kill.' % str(requestDetails['RequestStatus']), '', 500)

        try:
            Utilities.changeStatus(lastSubmission, 'aborted')
        except RuntimeError, re:
            import traceback
            self.postError(str(re), str(traceback.format_exc()), 500)

        return {"result": "ok"}

    def postRequest(self, requestName):
        """
        Checks the request n the body with one arg, and changes the status with kwargs
        """
        result = {}
        body = cherrypy.request.body.read()
        requestSchema = {}

        ## we'll need to have specific exception handling
        try:
            requestSchema = unidecode(JsonWrapper.loads(body))
        except Exception, ex:
            msg = "Error: problem decoding the body of the request"
            self.postError(msg, str(body), 400)

        self.validateAsyncDest(requestSchema)

        requestSchema["CouchUrl"] =  removePasswordFromUrl(self.configCacheCouchURL)
        requestSchema["CouchDBName"] =  self.configCacheCouchDB
        requestSchema["ACDCUrl"] =  removePasswordFromUrl(self.ACDCCouchURL)
        requestSchema["ACDCDBName"] =  self.ACDCCouchDB

        #requestName must be unique. Unique name is the ID
        currentTime = time.strftime('%y%m%d_%H%M%S',
                                 time.localtime(time.time()))

        requestSchema['OriginalRequestName'] = requestSchema['RequestName']
        requestSchema['RequestName'] = "%s_%s_%s" % (requestSchema["Requestor"], requestSchema['RequestName'],
                                                  currentTime)
        requestSchema['Campaign'] = requestSchema['RequestName']
        requestSchema['Submission'] = 1
        result['ID'] = requestSchema['RequestName']

        # Figure out ProcessingVersion
        self.setProcessingVersion(requestSchema)
        result['ProcessingVersion'] = requestSchema['ProcessingVersion']

        maker = retrieveRequestMaker(requestSchema['RequestType'])
        specificSchema = maker.schemaClass()
        specificSchema.update(requestSchema)
        url = cherrypy.url()
        # we only want the first part, before /task/
        url = url[0:url.find('/task')]
        specificSchema.reqMgrURL = url

        # Can pile up error messages here for various checks\
        errorMessage = ''
        if requestSchema.get("ACDCDoc", None) and requestSchema['JobSplitAlgo'] != 'LumiBased':
            errorMessage += 'You must use LumiBased splitting if specifying a lumiMask. '

        if errorMessage:
            self.postError(errorMessage, str(body), 400)

        try:
            specificSchema.validate()
        except Exception, ex:
            self.postError(ex.message, str(ex), 400)

        request = maker(specificSchema)
        helper = WMWorkloadHelper(request['WorkflowSpec'])
        # can't save Request object directly, because it makes it hard to retrieve the _rev
        metadata = {}
        metadata.update(request)
        # don't want to JSONify the whole workflow
        del metadata['WorkflowSpec']
        workloadUrl = helper.saveCouch(self.couchUrl, self.workloadCouchDB, metadata=metadata)
        request['RequestWorkflow'] = removePasswordFromUrl(workloadUrl)
        request['PrepID'] = None

        # Auto Assign the requests
        ### what is the meaning of the Team in the Analysis use case?
        try:
            CheckIn.checkIn(request)
            ChangeState.changeRequestStatus(requestSchema['RequestName'], 'assignment-approved')
            ChangeState.assignRequest(requestSchema['RequestName'], requestSchema["Team"])
        except RuntimeError, re:
            self.postError(str(re), '', 500)

        #return  ID & status
        return result

    def reprocessRequest(self, requestName):
        """
        This takes a request and submit a new one starting from the original one
        Using a json body to pass resubmission variables
        """

        try:
            WMCore.Lexicon.requestName(requestName)
        except AssertionError, ex:
            self.postError("Invalid request name specified", '', 400)

        self.logger.info("Reprocessing request %s" % requestName)

        ## TODO mcinquil remove this and get it from the status
        campaignWfs = sorted([(req['value']['Submission'], req['value']['RequestName'], req['value']['OriginalRequestName']) for req in self.__getFromCampaign(requestName)])
        if not campaignWfs:
            self.postError('Request %s not found. Impossible to resubmit.' % requestName, '', 400)
        lastSubmission = campaignWfs[-1][1]
        firstSubmission = campaignWfs[0][1:]

        ## retrieving the original request status
        campaignStatus = self.getDetailedStatus(lastSubmission)['workflows']
        requestStatus = campaignStatus[map(itemgetter('request'), campaignStatus).index(lastSubmission)]

        if 'Unknown' in requestStatus['requestDetails'].get('RequestStatus', 'Unknown'):
            self.postError('Request %s not found. Impossible to resubmit.' % lastSubmission, str(requestStatus), 400)

        body = cherrypy.request.body.read()
        requestSchema = {}

        ## we'll need to have specific exception handling
        try:
            requestSchema = unidecode(JsonWrapper.loads(body))
        except Exception, ex:
            msg = "Error: problem decoding the body of the request"
            self.postError(msg, str(body) + '\n' + str(ex), 400)

        taskResubmit = requestSchema.get('TaskResubmit', 'Analysis')

        ## resubmitting just if there are failed jobs, valid for forced resubmissions too
        if requestStatus['states']['/' + lastSubmission + '/' + taskResubmit].get('failure', {'count': 0})['count'] < 1 :
            self.postError("Request '%s' doesn't have failed jobs." % lastSubmission, '', 400)

        ## not forced resubmission, just check workflow/job status
        if not requestSchema.get('ForceResubmit', False):
            ## resubmitting just if the previous request was failed
            if not requestStatus['requestDetails']['RequestStatus'] in ['completed']:
                self.postError("Request '%s' not yet completed; impossible to resubmit." % lastSubmission, '', 400)
        else:
            ## killing for any job status before resumitting kill the previous request
            try:
                ChangeState.changeRequestStatus(lastSubmission, 'aborted')
            except RuntimeError, re:
                self.postError(str(re), '', 500)

        ## retrieving the original request schema
        originalRequest = GetRequest.getRequestByName( lastSubmission )
        helper = loadWorkload(originalRequest)
        originalSchema = helper.data.request.schema.dictionary_()

        wmtask = helper.getTask(taskResubmit)
        if not wmtask:
            self.postError('%s task not found in the workflow %s.' %(taskResubmit, lastSubmission), '', 404)
        taskPath = wmtask.getPathName()

        # this below is taking parameter from the original request
        #   note: site-b/w-lists are replaced while we could probably decide to simply append them
        newPars = { 'OriginalRequestName': originalSchema['RequestName'],
                    'InitialTaskPath':     taskPath,
                    'SiteWhitelist':       requestSchema.get('SiteWhitelist', originalSchema.get('SiteWhitelist', [])),
                    'SiteBlacklist':       requestSchema.get('SiteBlacklist', originalSchema.get('SiteBlacklist', [])),
                    'Submission':          originalSchema['Submission'] + 1,
                  }

        ## at some point we'll have some other kind of request (MonteCarlo?)
        ## and we might need to have different parameters then analysis has
        if originalSchema['RequestType'] in ['Analysis']:
            newPars['ACDCServer'] = originalSchema['ACDCUrl']
            newPars['ACDCDatabase'] = originalSchema['ACDCDBName']

        ## creating the new schema obj for the resubmission
        resubmitSchema = originalSchema.copy()
        resubmitSchema.update( newPars )

        ## retrieving the resubmission maker - the generic one allows to update analayis specific parameters
        maker = retrieveRequestMaker("Resubmission")
        schema = maker.newSchema()
        ## updating it with resubmission parameters
        schema.update( resubmitSchema )
        ## generating a new name: I am choosing the original name plus resubmit and date
        currentTime = time.strftime('%y%m%d_%H%M%S', time.localtime(time.time()))
        schema['RequestName'] = "%s_%s_%s_%s" % (originalSchema['Requestor'], firstSubmission[1], 'resubmit', currentTime)
        request = maker(schema)
        newHelper = WMWorkloadHelper(request['WorkflowSpec'])
        # can't save Request object directly, because it makes it hard to retrieve the _rev
        metadata = {}
        metadata.update(request)
        # don't want to JSONify the whole workflow
        del metadata['WorkflowSpec']
        workloadUrl = newHelper.saveCouch(self.couchUrl, self.workloadCouchDB, metadata=metadata)
        request['RequestWorkflow'] = removePasswordFromUrl(workloadUrl)
        self.logger.debug("Injecting the request")
        try:
            CheckIn.checkIn(request)
            ChangeState.changeRequestStatus(schema['RequestName'], 'assignment-approved')
            ChangeState.assignRequest(schema['RequestName'], schema["Team"])
        except RuntimeError, re:
            self.postError(str(re), '', 500)

        ## returning this analogue to the normal submission
        return {'ID': schema['RequestName']}

    def addNewUser(self):
        """
        The client must pass the user DN.
        The server get the username from sitedb (using userDN)
        The user DN must be propagated to WMBS wmbs_user.name
        """

        body = cherrypy.request.body.read()
        requestorInfos = {}

        ## we'll need to have specific exception handling
        try:
            requestorInfos = unidecode(JsonWrapper.loads(body))
        except Exception, ex:
            msg = "Error: problem decoding the body of the request"
            self.postError(msg, str(body), 400)

        self.logger.info("Requestor information: %s" %str(requestorInfos))

        userDN = requestorInfos.get("UserDN", None)
        group = requestorInfos.get("Group", None)
        team = requestorInfos.get("Team", "Analysis")
        email = requestorInfos.get("Email", None)

        result = {}
        if userDN != None :
            mySiteDB = SiteDBJSON()
            ## we'll need to have specific exception handling
            try:
                user = mySiteDB.dnUserName(userDN)
            except Exception, ex:
                self.postError("Problem extracting user " + userDN + " from SiteDB", str(ex), 500)
        else:
            self.postError("Bad input userDN not defined", '', 400)
        if user != None:
            if email == None:
                self.postError("Bad input user email not defined", '', 400)
            if not Registration.isRegistered(user):
                Registration.registerUser(user, email, userDN)
            result['hn_name'] = '%s' % user
        if group != None:
            if not Information.groupExists(group):
                GroupManagement.addGroup(group)
                result['group'] = '% registered' % group
            else:
                result['group'] = '%s already registered' % group
        if group != None and user != None:
            factory = DBConnect.getConnection()
            idDAO = factory(classname = "Requestor.ID")
            userId = idDAO.execute(user)
            assocDAO = factory(classname = "Requestor.GetAssociation")
            assoc = assocDAO.execute(userId)
            if len(assoc) == 0:
                GroupManagement.addUserToGroup(user, group)
        if team != None:
            if not ProdManagement.getTeamID(team):
                ProdManagement.addTeam(team)
                result['team'] = '%s registered' % team
            else:
                result['team'] = '%s already registered' % team
        else:
            self.postError("Bad input Team not defined", '', 400)

        return result

    def getDetailedStatus(self, requestName):
        """
        Get the status with job counts, etc
        """
        self.logger.info("Getting detailed status info for request %s" % requestName)

        try:
            WMCore.Lexicon.requestName(requestName)
        except AssertionError, ex:
            self.postError("Invalid request name specified", '', 400)

        campaignWfs = sorted([(work['value']['Submission'], work['value']['RequestName']) for work in self.__getFromCampaign(requestName)], key=lambda wf: wf[0])
        campaignStatus = []

        try:
            self.logger.debug("Connecting to database %s using the couch instance at %s: " % (self.jsmCacheCouchDB, self.jsmCacheCouchURL))
            self.couchdb = CouchServer(self.jsmCacheCouchURL)
            self.jobDatabase = self.couchdb.connectDatabase("%s/jobs" % self.jsmCacheCouchDB)
        except CouchError, ex:
            self.postError("Error connecting to couch database", str(ex) + '\n' + str(getattr(ex, 'reason', '')), 500)

        totJobs = 0
        for wfCounter, singleWf in campaignWfs:
            options = {"reduce": False, "startkey": [singleWf], "endkey": [singleWf, {}] }
            jobResults = self.jobDatabase.loadView("JobDump", "statusByWorkflowName", options)
            self.logger.debug("Found %d rows in the jobs database." % len(jobResults["rows"]))

            try:
                requestDetails = GetRequest.getRequestDetails(singleWf)
            except RuntimeError:
                requestDetails = {'RequestStatus':'Unknown, ReqMgr unreachable'}

            requestDetails = {'RequestMessages': requestDetails['RequestMessages'], 'RequestStatus': requestDetails['RequestStatus']}

            jobList = [int(row['value']['jobid']) for row in jobResults['rows']]
            jobList.sort()

            stateDict = {}

            for row in jobResults['rows']:
                jobID = row['value']['jobid']
                state = row['value']['state']
                task  = row['value']['task']
                jobNum = jobList.index(jobID) + 1 + totJobs

                if stateDict.has_key(task):
                    if stateDict[task].has_key(state):
                        stateDict[task][state]['count'] += 1
                        stateDict[task][state]['jobs'].append(jobNum)
                        stateDict[task][state]['jobIDs'].append(jobID)
                    else:
                        stateDict[task][state] = {'count': 1, 'jobs': [jobNum], 'jobIDs': [jobID]}
                else:
                    stateDict[task] = {state : {'count': 1, 'jobs': [jobNum], 'jobIDs': [jobID]} }
            campaignStatus.append( {'request': singleWf, 'subOrder': wfCounter, 'states': stateDict, 'requestDetails': requestDetails} )
            totJobs += len( jobResults['rows'] )

        return {'workflows': campaignStatus}

    def getDataLocation(self, requestName, jobRange):
        """
        Load output PFNs by Workflow and return {JobID:PFN}
        """
        try:
            WMCore.Lexicon.requestName(requestName)
        except AssertionError, ex:
            self.postError("Invalid request name specified", '', 400)

        campaignWfs = sorted([(work['value']['Submission'], work['value']['RequestName']) for work in self.__getFromCampaign(requestName)], key=lambda wf: wf[0])

        self.logger.info("Getting Data Locations for request %s and jobs range %s" % (requestName, str(jobRange)))

        try:
            WMCore.Lexicon.jobrange(jobRange)
        except AssertionError, ex:
            self.postError("Bad range of jobs specified", '', 400)
        jobRange = getJobsFromRange(jobRange)

        try:
            self.logger.debug("Connecting to database %s/fwjrs using the couch instance at %s: " % (self.jsmCacheCouchDB, self.jsmCacheCouchURL))
            self.couchdb = CouchServer(self.jsmCacheCouchURL)
            self.fwjrdatabase = self.couchdb.connectDatabase("%s/fwjrs" % self.jsmCacheCouchDB)
        except CouchError, ex:
            self.postError("Error connecting to couch database", str(ex) + '\n' + str(getattr(ex, 'reason', '')), 500)

        result = {'data': []}
        jobList = []
        for wfCounter, singleWf in campaignWfs:
            jobList.extend( self.jobList(singleWf) )
            options = {"startkey": [singleWf], "endkey": [singleWf] }
            fwjrResults = self.fwjrdatabase.loadView("FWJRDump", "outputLFNByWorkflowName", options)
            self.logger.debug("Found %d rows in the fwjrs database." % len(fwjrResults["rows"]))
            result['data'].append({'request': singleWf, 'subOrder': wfCounter, 'output': self.extractLFNs(fwjrResults, jobRange, jobList)})

        return result

    def getLogLocation(self, requestName, jobRange):
        """
        Load log LFNs by Workflow and return {JobID:PFN}
        """
        try:
            WMCore.Lexicon.requestName(requestName)
        except AssertionError, ex:
            self.postError("Invalid request name specified", '', 400)
        try:
            WMCore.Lexicon.jobrange(jobRange)
        except AssertionError, ex:
            self.postError("Please specify a valid range of jobs", str(ex), 400)

        self.logger.info("Getting Logs Locations for request %s and jobs range %s" % (requestName, str(jobRange)))
        campaignWfs = sorted([(work['value']['Submission'], work['value']['RequestName']) for work in self.__getFromCampaign(requestName)], key=lambda wf: wf[0])
        jobRange = getJobsFromRange(jobRange)

        try:
            self.logger.debug("Connecting to database %s using the couch instance at %s: " % (self.jsmCacheCouchDB, self.jsmCacheCouchURL))
            self.couchdb = CouchServer(self.jsmCacheCouchURL)
            self.fwjrdatabase = self.couchdb.connectDatabase("%s/fwjrs" % self.jsmCacheCouchDB)
        except CouchError, ex:
            self.postError("Error connecting to couch database", str(ex) + '\n' + str(getattr(ex, 'reason', '')), 500)

        result = {'log': []}
        jobList = []
        for wfCounter, singleWf in campaignWfs:
            jobList.extend( self.jobList(singleWf) )
            options = {"startkey": [singleWf], "endkey": [singleWf] }
            fwjrResults = self.fwjrdatabase.loadView("FWJRDump", "logArchivesLFNByWorkflowName", options)
            self.logger.debug("Found %d rows in the fwjrs database." % len(fwjrResults["rows"]))
            result['log'].append({'request': singleWf, 'subOrder': wfCounter, 'output': self.extractLFNs(fwjrResults, jobRange, jobList)})

        return result

    def getGoodLumis(self, requestName):
        """
        Return the list of good lumis processed as generated
        by CouchDB
        """
        try:
            WMCore.Lexicon.requestName(requestName)
        except AssertionError, ex:
            self.postError("Invalid request name specified", '', 400)

        self.logger.info("Getting list of processed lumis for request %s" % requestName)

        campaignWfs = sorted([(work['value']['Submission'], work['value']['RequestName']) for work in self.__getFromCampaign(requestName)], key=lambda wf: wf[0])

        try:
            self.logger.debug("Connecting to database %s using the couch instance at %s: " % (self.jsmCacheCouchDB, self.jsmCacheCouchURL))
            self.couchdb = CouchServer(self.jsmCacheCouchURL)
            self.fwjrdatabase = self.couchdb.connectDatabase("%s/fwjrs" % self.jsmCacheCouchDB)
        except CouchError, ex:
            self.postError("Error connecting to couch database", str(ex) + '\n' + str(getattr(ex, 'reason', '')), 500)

        result = {'lumis': []}
        for wfCounter, singleWf in campaignWfs:
            keys = [singleWf]
            result['lumis'].append({'request': singleWf, 'subOrder': wfCounter, 'lumis': self.fwjrdatabase.loadList("FWJRDump", "lumiList", "goodLumisByWorkflowName", keys=keys)})

        return result

    def extractLFNs(self, fwjrResults, jobRange, jobList):
        """
        Pull the list of LFNs,locations out of a couch view, convert them into the pfn, return it!
        """
        def foundIt():
            #id are like 127-1 (id-retrycount). Taking the retrycount
            retryCountMap[jobKey] = f['id'].split('-')[1]
            try:
                pfn = self.converter.lfn2pfn(f['value']['location'], f['value']['lfn'])
            except Exception,ex:
                self.logger.exception()
                msg = "Error converting lfn to pfn. LFN: %s, location: %s" % (f['value']['lfn'], f['value']['location'])
                self.postError("Error converting lfn to pfn.", str(ex) + '\n' + str(getattr(ex, 'reason', '')), 500)
            result[jobKey] = { 'pfn': pfn }
            if f['value'].has_key('checksums'):
                result[jobKey].update( {'checksums':f['value']['checksums']})

        result = {}
        # This map will keep track of the highest job retry-count. Elements are like { jobid : retrycount }
        retryCountMap = {}
        for f in fwjrResults["rows"]:
            jobID = int(f['value']['jobid'])
            if jobID in jobList and f['value'].has_key('lfn'):
                jobNum = jobList.index(jobID) + 1
                if jobNum in jobRange:
                    jobKey = str(jobNum)
                    self.logger.debug("Found file for jobID %s at index %s" % (jobID, jobNum))
                    #check the retry count
                    if result.has_key(jobKey):
                        oldRetryCount = int(retryCountMap[jobKey])
                        currRetryCount = int(f['id'].split('-')[1])
                        #insert in the result dict only if it is a new retry
                        if currRetryCount > oldRetryCount:
                            foundIt()
                    else:
                        foundIt()

        self.logger.debug("PFN dict %s" % result)
        return result

    @restexpose
    def uploadUserSandbox(self, userfile, checksum, doUpload=1):
        """
        Receive the upload of the user sandbox and forward on to UserFileCache
        if needed
        """
        ufcHost = 'http://%s:%s/' % (self.sandBoxCacheHost, self.sandBoxCachePort)
        doUpload = (doUpload != '0')

        # Calculate the hash of the file
        try:
            tar = tarfile.open(fileobj=userfile.file, mode='r')
            lsl = [(x.name, int(x.size), int(x.mtime), x.uname) for x in tar.getmembers()]
            hasher = hashlib.sha256(str(lsl))
            digest = hasher.hexdigest()
        except tarfile.ReadError:
            raise cherrypy.HTTPError(400, 'File is not a .tgz file.')

        # Basic preservation of the file integrity
        if not (digest == checksum):
            msg = "File transfer error: digest check failed between %s and %s"  % (digest, checksum)
            self.postError(msg, "", 400)

        # See if the server already has this file
        if doUpload:
            userFileCache = JSONRequests(url=ufcHost)
            existsResult = userFileCache.get(uri=self.sandBoxCacheBasepath+'/exists', data={'hashkey':digest})
            if existsResult[0]['exists']:
                self.logger.debug("Sandbox %s already exists" % digest)
                return existsResult[0]

        # Not on server, make a local copy for curl
        downloadUrl = 'Upload not attempted'
        with tempfile.NamedTemporaryFile() as uploadHandle:
            userfile.file.seek(0)
            shutil.copyfileobj(userfile.file, uploadHandle)
            uploadHandle.flush()

            url = '%s%s/upload' % (ufcHost, self.sandBoxCacheBasepath)
            if doUpload:
                self.logger.debug("Uploading user sandbox %s to %s" % (digest, url))
                # Upload the file to UserFileCache
                with tempfile.NamedTemporaryFile() as curlOutput:
                    curlCommand = 'curl -H "Accept: application/json" -F"checksum=%s" -F userfile=@%s %s -o %s' % \
                                (digest, uploadHandle.name, url, curlOutput.name)
                    (status, output) = commands.getstatusoutput(curlCommand)
                    returnDict = json.loads(curlOutput.read())
                    size = returnDict['size']
                    downloadUrl= returnDict['url']
                    digest = returnDict['hashkey']

        results = {'size':size, 'hashkey':digest, 'url':downloadUrl}
        return results

    def validateAsyncDest(self, request):
        """
        Make sure asyncDest is there, has the right format, and is a valid site
        """
        if not request.get('asyncDest', None):
            self.postError("asyncDest parameter is missing from request", '', 400)

        asyncDest = request['asyncDest']
        try:
            if not WMCore.Lexicon.cmsname(asyncDest):
                msg = 'asyncDest parameter (%s) is not a valid CMS site name' % asyncDest
                self.postError(msg, '', 400)
        except AssertionError:
            msg = 'asyncDest parameter (%s) is not a valid CMS site name' % asyncDest
            self.postError(msg, '', 400)

        se = None
        try:
            se = SiteDBJSON().cmsNametoSE(asyncDest)
        except (RuntimeError, SyntaxError), er:
            self.postError( str(er), '', 500)

        if len(se) < 1:
            msg = 'asyncDest parameter (%s) is not a valid CMS site name or has no associated SE' % asyncDest
            self.postError(msg, '', 400)

        return True

    def jobList(self, requestName):
        """
        Return a list of job IDs in order to aid in correlating user job # with JobID
        """

        try:
            self.couchdb = CouchServer(self.jsmCacheCouchURL)
            self.jobDatabase = self.couchdb.connectDatabase("%s/jobs" % self.jsmCacheCouchDB)
        except CouchError, ex:
            self.postError("Error connecting to couch database", str(ex) + '\n' + str(getattr(ex, 'reason', '')), 500)

        options = {"reduce": False, "startkey": [requestName], "endkey": [requestName, {}] }
        jobResults = self.jobDatabase.loadView("JobDump", "statusByWorkflowName", options)
        self.logger.debug("Found %d rows in the jobs database." % len(jobResults["rows"]))

        # Sort the jobs numerically
        jobList = [int(row['value']['jobid']) for row in jobResults['rows']]
        jobList.sort()

        return jobList

    def getClientMapping(self):
        """
        Return the dictionary that allows the client to map the client configuration to the server request
        It also returns the URI for each API
        """

        return self.clientMapping

    def getJobErrors(self, requestName):
        """
        Return all the error reasons for each job in the workflow
        """
        try:
            WMCore.Lexicon.requestName(requestName)
        except AssertionError, ex:
            self.postError("Invalid request name specified", '', 400)

        self.logger.info("Getting failed reasons for jobs in request %s" % requestName)

        campaignWfs = sorted([(work['value']['Submission'], work['value']['RequestName']) for work in self.__getFromCampaign(requestName)], key=lambda wf: wf[0])

        try:
            self.logger.debug("Connecting to database %s/fwjrs using the couch instance at %s: " % (self.jsmCacheCouchDB, self.jsmCacheCouchURL))
            self.couchdb = CouchServer(self.jsmCacheCouchURL)
            self.fwjrdatabase = self.couchdb.connectDatabase("%s/fwjrs" % self.jsmCacheCouchDB)
        except CouchError, ex:
            self.postError("Error connecting to couch database", str(ex) + '\n' + str(getattr(ex, 'reason', '')), 500)

        result = {'errors': []}
        for wfCounter, singleWf in campaignWfs:
            options = {"startkey": [singleWf], "endkey": [singleWf, {}] }
            jobResults = self.fwjrdatabase.loadView("FWJRDump", "errorsByWorkflowName", options)
            self.logger.debug("Found %d rows in the fwjrs database." % jobResults["total_rows"])
            ## retrieving relative's job id in the request
            jobList = self.jobList(singleWf)
            ## formatting the result
            dictresult = {}
            for failure in jobResults['rows']:
                primkeyjob = str( jobList.index(failure['value']['jobid']) + 1 )
                secokeyretry = str(failure['value']['retry'])
                ## we may have already added the job due to another failure in another submission
                if not primkeyjob in dictresult:
                    dictresult[primkeyjob] = {secokeyretry: {}}
                ## we may have already added a failure for the same retry (eg: in a different step)
                if not secokeyretry in dictresult[primkeyjob]:
                    dictresult[primkeyjob][secokeyretry] = {}
                dictresult[primkeyjob][secokeyretry][str(failure['value']['step'])] = failure['value']['error']
            result['errors'].append({'request': singleWf, 'subOrder': wfCounter, 'details': dictresult})

        return result

    def setProcessingVersion(self, request):
        """
        If no ProcessingVersion is specified, go to couch to figure out next one.
        """

        if request.get('ProcessingVersion', None):
            return

        try:
            self.logger.debug("Connecting to database %s using the couch instance at %s: " % (self.workloadCouchDB, self.couchUrl))
            self.couchdb = CouchServer(self.couchUrl)
            self.database = self.couchdb.connectDatabase(self.workloadCouchDB)
        except CouchError, ex:
            self.postError("Error connecting to couch database", str(ex) + '\n' + str(getattr(ex, 'reason', '')), 500)

        startkey = [request['Requestor'], request['PublishDataName'], request['InputDataset']]
        endkey = copy.copy(startkey)
        endkey.append({})
        options = {"startkey" : startkey, "endkey" : endkey}

        requests = self.database.loadView("ReqMgr", "requestsByUser", options)
        self.logger.debug("Found %d rows in the requests database." % requests["total_rows"])

        versions = []
        for row in requests['rows']:
            oldVersion = row['value']['version']
            try:
                versions.append(int(oldVersion.replace('v', '')))
            except ValueError: # Not an int, so we ignore it
                pass

        self.logger.debug("Existing versions for workflow are: %s" % versions)
        newVersion = 1
        if versions:
            newVersion = max(versions) + 1

        self.logger.debug("New version is: %s" % newVersion)
        request['ProcessingVersion'] = 'v%d' % newVersion
