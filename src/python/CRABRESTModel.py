"""
CRAB Interface to the WMAgent
"""

import cherrypy
import commands
import hashlib
import shutil
import tempfile
import threading
import time
import json
import imp

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
                        args=['requestID'],
                        validation=[self.isalnum])
        self._addMethod('PUT', 'task', self.putTaskModifies,
                        args=['requestID'],
                        validation=[self.isalnum])
        self._addMethod('DELETE', 'task', self.deleteRequest,
                        args=['requestID'],
                        validation=[self.isalnum])
        self._addMethod('POST', 'task', self.postRequest,
                        args=['requestName'],
                        validation=[self.isalnum])
        #/config
        self._addMethod('POST', 'config', self.postUserConfig,
                        args=[],
                        validation=[self.checkConfig])
        #/data
        self._addMethod('GET', 'data', self.getDataLocation,
                       args=['requestID','jobRange'], validation=[self.checkConfig])

        #/goodLumis, equivalent of "report" from CRAB2
        self._addMethod('GET', 'goodLumis', self.getGoodLumis,
                       args=['requestID'], validation=[self.checkConfig])

        #/lumiMask
        self._addMethod('POST', 'lumiMask', self.postLumiMask,
                       args=[], validation=[self.checkConfig])

        #/log
        self._addMethod('GET', 'log', self.getLogLocation,
                       args=['requestID','jobRange'], validation=[self.checkConfig])

        # Server
        self._addMethod('GET', 'info', self.getServerInfo,
                        args=[],
                        validation=[self.isalnum])

        self._addMethod('GET', 'requestmapping', self.getClientMapping,
                        args=[],
                        validation=[self.isalnum])

        # this allows to retrieve failure reason for each job
        self._addMethod('GET', 'jobErrors', self.getJobErrors,
                        args=['requestID'],
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
        self.logger.error(errmsg)
        self.logger.debug(dbgmsg)
        raise cherrypy.HTTPError(code, errmsg)

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

    def putTaskModifies(self, requestID):
        """
        Modify the task in any possible field : B/W lists, stop/start automation...
        """
        self.postError("Not implemented", '', 501)
        return requestID

    def deleteRequest(self, requestID):
        """
        Delete a request identified by requestID
        """
        self.postError("Not implemented", '', 501)
        return requestID

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
        result['ID'] = requestSchema['RequestName']

        maker = retrieveRequestMaker(requestSchema['RequestType'])
        specificSchema = maker.schemaClass()
        specificSchema.update(requestSchema)
        url = cherrypy.url()
        # we only want the first part, before /task/
        url = url[0:url.find('/task')]
        specificSchema.reqMgrURL = url

        ## we'll need to have specific exception handling
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

        ## we'll need to have specific exception handling
        try:
            CheckIn.checkIn(request)
        except Exception, ex:
            self.postError("Problem checking in the request", str(ex), 500)

        # Auto Assign the requests
        ## we'll need to have specific exception handling
        try:
            ChangeState.changeRequestStatus(requestSchema['RequestName'], 'assignment-approved')
        except Exception, ex:
            self.postError("Problem changing the request status", str(ex), 500)

        # Auto Assign the requests
        ### what is the meaning of the Team in the Analysis use case?
        ## we'll need to have specific exception handling
        try:
            ChangeState.assignRequest(requestSchema['RequestName'], requestSchema["Team"])
        except Exception, ex:
            self.postError("Problem changing the request assignment", str(ex), 500)
        #return  ID & status
        return result


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

    def getDetailedStatus(self, requestID):
        """
        Get the status with job counts, etc
        """
        self.logger.info("Getting detailed status info for request %s" % (requestID))

        try:
            self.logger.debug("Connecting to database %s using the couch instance at %s: " % (self.jsmCacheCouchDB, self.jsmCacheCouchURL))
            self.couchdb = CouchServer(self.jsmCacheCouchURL)
            self.jobDatabase = self.couchdb.connectDatabase("%s/jobs" % self.jsmCacheCouchDB)
        except CouchError, ex:
            self.postError("Error connecting to couch database", str(ex) + '\n' + str(getattr(ex, 'reason', '')), 500)

        options = {"reduce": False, "startkey": [requestID], "endkey": [requestID, {}] }
        jobResults = self.jobDatabase.loadView("JobDump", "statusByWorkflowName", options)
        self.logger.debug("Found %d rows in the jobs database." % len(jobResults["rows"]))

        try:
            requestDetails = GetRequest.getRequestDetails(requestID)
        except RuntimeError:
            # TODO: Drop percent_success on ticket #2035
            requestDetails = {'RequestStatus':'Unknown, ReqMgr unreachable', 'percent_success':0}

        jobList = [int(row['value']['jobid']) for row in jobResults['rows']]
        jobList.sort()

        stateDict = {}

        for row in jobResults['rows']:
            jobID = row['value']['jobid']
            state = row['value']['state']
            jobNum = jobList.index(jobID) + 1

            if stateDict.has_key(state):
                stateDict[state]['count'] += 1
                stateDict[state]['jobs'].append(jobNum)
                stateDict[state]['jobIDs'].append(jobID)
            else:
                stateDict[state] = {'count':1, 'jobs':[jobNum], 'jobIDs':[jobID]}

        return {'states':stateDict, 'requestDetails':requestDetails}

    def getDataLocation(self, requestID, jobRange):
        """
        Load output PFNs by Workflow and return {JobID:PFN}
        """
        self.logger.info("Getting Data Locations for request %s and jobs range %s" % (requestID, str(jobRange)))

        try:
            WMCore.Lexicon.jobrange(jobRange)
        except AssertionError, ex:
            self.postError("Bad range of jobs specified", '', 400)
        jobRange = getJobsFromRange(jobRange)
        jobList = self.jobList(requestID)

        try:
            self.logger.debug("Connecting to database %s using the couch instance at %s: " % (self.jsmCacheCouchDB, self.jsmCacheCouchURL))
            self.couchdb = CouchServer(self.jsmCacheCouchURL)
            self.fwjrdatabase = self.couchdb.connectDatabase("%s/fwjrs" % self.jsmCacheCouchDB)
        except CouchError, ex:
            self.postError("Error connecting to couch database", str(ex) + '\n' + str(getattr(ex, 'reason', '')), 500)

        options = {"startkey": [requestID], "endkey": [requestID] }

        fwjrResults = self.fwjrdatabase.loadView("FWJRDump", "outputPFNByWorkflowName", options)
        self.logger.debug("Found %d rows in the fwjr database." % len(fwjrResults["rows"]))

        return self.extractPFNs(fwjrResults, jobRange, jobList)

    def getLogLocation(self, requestID, jobRange):
        """
        Load log PFNs by Workflow and return {JobID:PFN}
        """
        self.logger.info("Getting Logs Locations for request %s and jobs range %s" % (requestID, str(jobRange)))

        try:
            WMCore.Lexicon.jobrange(jobRange)
        except AssertionError, ex:
            self.postError("Please specify a valid range of jobs", str(ex), 400)

        jobRange = getJobsFromRange(jobRange)
        jobList = self.jobList(requestID)

        try:
            self.logger.debug("Connecting to database %s using the couch instance at %s: " % (self.jsmCacheCouchDB, self.jsmCacheCouchURL))
            self.couchdb = CouchServer(self.jsmCacheCouchURL)
            self.fwjrdatabase = self.couchdb.connectDatabase("%s/fwjrs" % self.jsmCacheCouchDB)
        except CouchError, ex:
            self.postError("Error connecting to couch database", str(ex) + '\n' + str(getattr(ex, 'reason', '')), 500)

        options = {"startkey": [requestID], "endkey": [requestID] }

        fwjrResults = self.fwjrdatabase.loadView("FWJRDump", "logArchivesPFNByWorkflowName", options)

        return self.extractPFNs(fwjrResults, jobRange, jobList)


    def getGoodLumis(self, requestID):
        """
        Return the list of good lumis processed as generated
        by CouchDB
        """
        self.logger.info("Getting list of processed lumis for request %s" % (requestID))

        try:
            self.logger.debug("Connecting to database %s using the couch instance at %s: " % (self.jsmCacheCouchDB, self.jsmCacheCouchURL))
            self.couchdb = CouchServer(self.jsmCacheCouchURL)
            self.fwjrdatabase = self.couchdb.connectDatabase("%s/fwjrs" % self.jsmCacheCouchDB)
        except CouchError, ex:
            self.postError("Error connecting to couch database", str(ex) + '\n' + str(getattr(ex, 'reason', '')), 500)

        keys = [requestID]

        goodLumis = self.fwjrdatabase.loadList("FWJRDump", "lumiList", "goodLumisByWorkflowName", keys=keys)
        return goodLumis

    def extractPFNs(self, fwjrResults, jobRange, jobList):
        """
        Pull the list of PFNs out of a couch view
        """
        result = {}
        # This map will keep track of the highest job retry-count. Elements are like { jobid : retrycount }
        retryCountMap = {}
        for f in fwjrResults["rows"]:
            currID = f['value']['jobid']
            jobID = int(currID)
            if jobID in jobList and f['value'].has_key('pfn'):
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
                            #id are like 127-1 (id-retrycount). Taking the retrycount
                            retryCountMap[jobKey] = f['id'].split('-')[1]
                            result[jobKey] = { 'pfn':f['value']['pfn'] }
                            if f['value'].has_key('checksums'):
                                result[jobKey].update( {'checksums':f['value']['checksums']})
                    else:
                        #id are like 127-1 (id-retrycount). Taking the retrycount
                        retryCountMap[jobKey] = f['id'].split('-')[1]

                        result[jobKey] = {'pfn':f['value']['pfn']}
                        if f['value'].has_key('checksums'):
                            result[jobKey].update( {'checksums':f['value']['checksums']})

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
        hasher = hashlib.sha256()

        size = 0
        while True:
            data = userfile.file.read(8192)
            if not data:
                break
            hasher.update(data)
            size += len(data)
        digest = hasher.hexdigest()

        # Basic preservation of the file integrity
        if not (digest == checksum):
            msg = "File transfer error: digest check failed between %s and %s"
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

        se = SiteDBJSON().cmsNametoSE(asyncDest)
        if len(se) < 1:
            msg = 'asyncDest parameter (%s) is not a valid CMS site name or has no associated SE' % asyncDest
            self.postError(msg, '', 400)

        return True

    def jobList(self, requestID):
        """
        Return a list of job IDs in order to aid in correlating user job # with JobID
        """

        try:
            self.couchdb = CouchServer(self.jsmCacheCouchURL)
            self.jobDatabase = self.couchdb.connectDatabase("%s/jobs" % self.jsmCacheCouchDB)
        except CouchError, ex:
            self.postError("Error connecting to couch database", str(ex) + '\n' + str(getattr(ex, 'reason', '')), 500)

        options = {"reduce": False, "startkey": [requestID], "endkey": [requestID, {}] }
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

    def getJobErrors(self, requestID):
        """
        Return all the error reasons for each job in the workflow
        """
        self.logger.debug("Getting failed reasons for jobs in request %s" % requestID)

        try:
            self.logger.debug("Connecting to database %s using the couch instance at %s: " % (self.jsmCacheCouchDB, self.jsmCacheCouchURL))
            self.couchdb = CouchServer(self.jsmCacheCouchURL)
            self.jobDatabase = self.couchdb.connectDatabase("%s/fwjrs" % self.jsmCacheCouchDB)
        except CouchError, ex:
            self.postError("Error connecting to couch database", str(ex) + '\n' + str(getattr(ex, 'reason', '')), 500)

        options = {"startkey": [requestID], "endkey": [requestID, {}] }
        jobResults = self.jobDatabase.loadView("FWJRDump", "errorsByWorkflowName", options)
        self.logger.debug("Found %d rows in the jobs database." % jobResults["total_rows"])

        ## retrieving relative's job id in the request
        jobList = self.jobList(requestID)

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

        return dictresult
