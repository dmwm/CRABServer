import time
import logging

from WMCore.WebTools.RESTModel import RESTModel
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import unidecode, removePasswordFromUrl
from WMCore.RequestManager.RequestMaker.Registry import retrieveRequestMaker
from WMCore.Database.CMSCouch import CouchServer
import WMCore.RequestManager.RequestDB.Connection as DBConnect
import WMCore.Wrappers.JsonWrapper as JsonWrapper
import WMCore.Lexicon
from WMCore.RequestManager.RequestMaker import CheckIn
import WMCore.RequestManager.RequestMaker.Processing.AnalysisRequest
from WMCore.RequestManager.RequestDB.Interface.Request import ChangeState, GetRequest
from WMCore.RequestManager.RequestDB.Interface.User import Registration
from WMCore.RequestManager.RequestDB.Interface.Group import Information
from WMCore.RequestManager.RequestDB.Interface.Admin import GroupManagement, ProdManagement
from WMCore.Cache.WMConfigCache import ConfigCache
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
import hashlib
import cherrypy
import threading
import json
import cherrypy

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
        self.configCacheCouchURL = config.configCacheCouchURL
        self.configCacheCouchDB = config.configCacheCouchDB
        self.jsmCacheCouchURL = config.jsmCacheCouchURL
        self.jsmCacheCouchDB = config.jsmCacheCouchDB
        self.agentDN = config.agentDN
        self.SandBoxCache_endpoint = config.SandBoxCache_endpoint
        self.SandBoxCache_port = config.SandBoxCache_port
        self.SandBoxCache_basepath = config.SandBoxCache_basepath

        #/user
        self._addMethod('POST', 'user', self.addNewUser,
                        args=[],
                        validation=[self.isalnum])
        #/task
        self._addMethod('GET', 'task', self.getTaskStatus,
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

        #/log
        self._addMethod('GET', 'log', self.getLogLocation,
                       args=['requestID','jobRange'], validation=[self.checkConfig])

        # Server
        self._addMethod('GET', 'info', self.getServerInfo,
                        args=[],
                        validation=[self.isalnum])

        cherrypy.engine.subscribe('start_thread', self.initThread)


    def initThread(self, thread_index):
        """
        The ReqMgr expects the DBI to be contained in the Thread
        """
        myThread = threading.currentThread()
        #myThread = cherrypy.thread_data
        # Get it from the DBFormatter superclass
        myThread.dbi = self.dbi

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
        params = unidecode(JsonWrapper.loads(body))

        configCache = ConfigCache(self.configCacheCouchURL, self.configCacheCouchDB)
        try:
            user = SiteDBJSON().dnUserName(params['UserDN'])
        except Exception, ex:
            raise cherrypy.HTTPError(500, "Problem extracting user from SiteDB %s" % str(ex))

        configCache.createUserGroup(params['Group'], user)

        configMD5 = hashlib.md5(params['ConfFile']).hexdigest()
        configCache.document['md5_hash'] = configMD5
        configCache.document['pset_hash'] = params['PsetHash']
        configCache.attachments['configFile'] = params['ConfFile']

        configCache.setPSetTweaks(json.loads(params['PsetTweaks']))

        logging.error(params['PsetTweaks'])

        configCache.setLabel(params['Label'])
        configCache.setDescription(params['Description'])
        configCache.save()

        result = {}
        result['DocID']  = configCache.document["_id"]
        result['DocRev'] = configCache.document["_rev"]

        return result

    def getServerInfo(self):
        """
        Return informatiion to allow client operations
        """

        result = {}
        ## could be a list of all supported DN
        result['server_dn']  = self.agentDN
        result['my_proxy'] = 'myproxy.cern.ch'
        result['sandbox'] = {}
        result['sandbox']['type'] = 'gridFtp'
        # the following will change as soon as we follow up on #1305
        # at the moment is gridFTP specific.
        result['sandbox']['endpoint'] = self.SandBoxCache_endpoint
        result['sandbox']['port'] = self.SandBoxCache_port
        result['sandbox']['basepath'] = self.SandBoxCache_basepath

        return result

    def isalnum(self, call_input):
        """
        Validates that all input is alphanumeric, with spaces and underscores
        tolerated.
        """
        for v in call_input.values():
            WMCore.Lexicon.identifier(v)
        return call_input

    def getTaskStatus(self, requestID):
        """
        Return the whole task status
        """
        requestDetails = GetRequest.getRequestDetails(requestID)
        return requestDetails

    def putTaskModifies(self, requestID):
        """
        Modify the task in any possible field : B/W lists, stop/start automation...
        """
        return requestID

    def deleteRequest(self, requestID):
        """
        Delete a request identified by requestID
        """
        return requestID

    def postRequest(self, requestName):
        """
        Checks the request n the body with one arg, and changes the status with kwargs
        """
        result = {}
        body = cherrypy.request.body.read()
        requestSchema = unidecode(JsonWrapper.loads(body))
        logging.error(requestSchema)

        requestSchema["CouchUrl"] =  self.configCacheCouchURL
        requestSchema["CouchDBName"] =  self.configCacheCouchDB

        #requestName must be unique. Unique name is the ID
        currentTime = time.strftime('%y%m%d_%H%M%S',
                                 time.localtime(time.time()))

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
        try:
            specificSchema.validate()
        except Exception, ex:
            raise cherrypy.HTTPError(400, ex.message)

        request = maker(specificSchema)
        helper = WMWorkloadHelper(request['WorkflowSpec'])
        # can't save Request object directly, because it makes it hard to retrieve the _rev
        metadata = {}
        metadata.update(request)
        # don't want to JSONify the whole workflow
        del metadata['WorkflowSpec']
        workloadUrl = helper.saveCouch(self.couchUrl, self.workloadCouchDB, metadata=metadata)
        request['RequestWorkflow'] = removePasswordFromUrl(workloadUrl)

        try:
            CheckIn.checkIn(request)
        except Exception, ex:
            raise cherrypy.HTTPError(500, str(ex))
        # Auto Assign the requests
        try:
            ChangeState.changeRequestStatus(requestSchema['RequestName'], 'assignment-approved')
        except Exception, ex:
            raise cherrypy.HTTPError(500, str(ex))
        # Auto Assign the requests
        ### what is the meaning of the Team in the Analysis use case?
        ChangeState.assignRequest(requestSchema['RequestName'], requestSchema["Team"])
        #return  ID & status
        return result


    def addNewUser(self):
        """
        The client must pass the user DN.
        The server get the username from sitedb (using userDN)
        The user DN must be propagated to WMBS wmbs_user.name
        """

        body = cherrypy.request.body.read()
        requestorInfos = unidecode(JsonWrapper.loads(body))

        logging.info(requestorInfos)

        userDN = requestorInfos.get("UserDN", None)
        group = requestorInfos.get("Group", None)
        team = requestorInfos.get("Team", "Analysis")
        email = requestorInfos.get("Email", None)

        result = {}
        if userDN != None :
            mySiteDB = SiteDBJSON()
            try:
                user = mySiteDB.dnUserName(userDN)
            except Exception, ex:
                raise cherrypy.HTTPError(500, "Problem extracting user from SiteDB %s" % str(ex))
        else:
            raise cherrypy.HTTPError(400, "Bad input userDN not defined")
        if user != None:
            if email == None: raise cherrypy.HTTPError(400, "Bad input user email not defined")
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
            raise cherrypy.HTTPError(400, "Bad input Team not defined")

        return result

    def getDataLocation(self, requestID, jobRange):
        """
        Load output PFNs by Workflow and return {JobID:PFN}
        """
        logging.info("Getting Data Locations for request %s and jobs range %s" % (requestID, str(jobRange)))

        try:
            WMCore.Lexicon.jobrange(jobRange)
        except AssertionError, ex:
	    raise cherrypy.HTTPError(400, "Bad range of jobs specified")
        jobRange = getJobsFromRange(jobRange)

        try:
            logging.debug("Connecting to database %s using the couch instance at %s: " % (self.jsmCacheCouchDB, self.jsmCacheCouchURL))
            self.couchdb = CouchServer(self.jsmCacheCouchURL)
            self.fwjrdatabase = self.couchdb.connectDatabase("%s/fwjrs" % self.jsmCacheCouchDB)
        except Exception, ex:
            raise cherrypy.HTTPError(400, "Error connecting to couch: %s" % str(ex))

        options = {"startkey": [requestID], "endkey": [requestID] }

        fwjrResults = self.fwjrdatabase.loadView("FWJRDump", "outputPFNByWorkflowName", options)
        logging.debug("Found %d rows in the fwjr database." % len(fwjrResults["rows"]))

        return self.extractPFNs(fwjrResults, jobRange)

    def getLogLocation(self, requestID, jobRange):
        """
        Load log PFNs by Workflow and return {JobID:PFN}
        """
        self.logger.info("Getting Logs Locations for request %s and jobs range %s" % (requestID, str(jobRange)))

        try:
            WMCore.Lexicon.jobrange(jobRange)
        except AssertionError, ex:
            raise cherrypy.HTTPError(400, "Please specify a valid range of jobs")

        jobRange = getJobsFromRange(jobRange)

        try:
            self.logger.debug("Connecting to database %s using the couch instance at %s: " % (self.jsmCacheCouchDB, self.jsmCacheCouchURL))
            self.couchdb = CouchServer(self.jsmCacheCouchURL)
            self.fwjrdatabase = self.couchdb.connectDatabase("%s/fwjrs" % self.jsmCacheCouchDB)
        except Exception, ex:
            raise cherrypy.HTTPError(400, "Error connecting to couch: %s" % str(ex))

        options = {"startkey": [requestID], "endkey": [requestID] }

        fwjrResults = self.fwjrdatabase.loadView("FWJRDump", "logArchivesPFNByWorkflowName", options)

        return self.extractPFNs(fwjrResults, jobRange)


    def extractPFNs(self, fwjrResults, jobRange):
        """
        """
        result = {}
        for f in fwjrResults["rows"]:
            currID = f['value']['jobid']
            if currID in jobRange and f['value'].has_key('pfn'):
                #check the retry count
                if result.has_key(currID):
                    oldJobId_RetryCount = result[currID][0]
                    oldRetryCount = int(oldJobId_RetryCount.split('-')[1])
                    currRetryCount = int(f['id'].split('-')[1])
                    #insert in the result dict only if it is a new retry
                    if currRetryCount > oldRetryCount:
                        result[currID] = [ f['id'] , f['value']['pfn'] ]
                else:
                    result[currID] = [ f['id'] , f['value']['pfn'] ]

        #Compact result deleting jobid-retrycount keys
        logging.debug("Preparing results")
        lightresult = {}
        for jobid, id_pfn in result.items():
            lightresult[ str(jobid) ] = id_pfn[1]

        return lightresult

