import time
import logging

from WMCore.WebTools.RESTModel import RESTModel
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import unidecode, removePasswordFromUrl
from WMCore.RequestManager.RequestMaker.Registry import retrieveRequestMaker
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
import hashlib
import cherrypy
import threading
import json
import cherrypy

class CRABRESTModel(RESTModel):
    """ CRAB Interface to the WMAgent """
    def __init__(self, config={}):
        '''
        Initialise the RESTModel and add some methods to it.
        '''
        RESTModel.__init__(self, config)
        #self.hostAddress = config.model.reqMgrHost
        self.couchUrl = config.model.couchUrl
        self.workloadCouchDB = config.model.workloadCouchDB
        self.configCacheCouchURL =  config.configCacheCouchURL
        self.configCacheCouchDB = config.configCacheCouchDB
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
                        args=[''],
                        validation=[self.checkConfig])

        # Server status
        self._addMethod('GET', 'status', self.getServerStatus)

        cherrypy.engine.subscribe('start_thread', self.initThread)


    def initThread(self, thread_index):
        """ The ReqMgr expects the DBI to be contained in the Thread  """
        myThread = threading.currentThread()
        #myThread = cherrypy.thread_data
        # Get it from the DBFormatter superclass
        myThread.dbi = self.dbi

    def checkConfig(self, pset):
        """ check user configuration """
        return pset

    def postUserConfig(self):
        """ this must act as proxy for CouchDB. Upload user config and return DocID """

        body = cherrypy.request.body.read()
        params = unidecode(JsonWrapper.loads(body))

        configCache = ConfigCache(self.confiCacheCouchURL, self.configCacheCouchDB)
        configCache.createUserGroup(params['groupName'], params['userName'])

        configMD5 = hashlib.md5(params['confFile']).hexdigest()
        configCache.document['md5_hash'] = configMD5
        configCache.document['pset_hash'] = params['psetHash']     #edmConfigHash
        configCache.attachments['configFile'] = params['confFile']

        configCache.setPSetTweaks(json.loads(params['psetTweaks']))
        configCache.setLabel(params['label'])
        configCache.setDescription(params['description'])
        configCache.save()

        result = {}
        result['DocID'] = configCache.id

        return result

    def getServerStatus(self):
        result = {
                'number_of_tasks':0,
                'my_proxy':'myproxy.cern.ch',
                'server_dn': '',
                'server_se_path': '/path/',
                'couch_url': "CONFIG_CACHE",
                'in_drain': False,
                'admin': "spiga"
                }
        return result

    def isalnum(self, index):
        """ Validates that all input is alphanumeric,
            with spaces and underscores tolerated"""
        for v in index.values():
            WMCore.Lexicon.identifier(v)
        return index

    def getTaskStatus(self, requestID):
        """ Return the whole task status  """
        requestDetails = GetRequest.getRequestDetails(requestID)
        return requestDetails


    def putTaskModifies(self, requestID):
        """ Modify the task in any possible field : B/W lists, stop/start automation... """
        return requestID

    def deleteRequest(self, requestID):
        """    """
        return requestID

    def postRequest(self, requestName):
        """ Checks the request n the body with one arg, and changes the status with kwargs """
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
            raise cherrypy.HTTPError(400,ex.message)

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
            raise cherrypy.HTTPError(500,ex.message)
        # Auto Assign the requests
        try:
            ChangeState.changeRequestStatus(requestSchema['RequestName'], 'assignment-approved')
        except Exception, ex:
            raise cherrypy.HTTPError(500,ex.message)
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
        user = requestorInfos.get("Username", None)
        group = requestorInfos.get("Group", None)
        team = requestorInfos.get("Team", "Analysis")
        email = requestorInfos.get("Email", None)

        result = {}
        if userDN != None :
            logging.info("userDN: To DO")
            # get hnName from it.
            # for the moment uses what the client pass..
        else:
            raise cherrypy.HTTPError(400, "Bad input userDN not defined")
        if user != None:
            if email == None: raise cherrypy.HTTPError(400, "Bad input user email not defined")
            if not Registration.isRegistered(user):
                Registration.registerUser(user, email,userDN)
                result['user'] = '%s registered'%user
            else:
                result['user'] = '%s already registered'%user
        if group != None:
            if not Information.groupExists(group):
                GroupManagement.addGroup(group)
                result['group'] = '% registered' %group
            else:
                result['group'] = '%s already registered'%group
        if group != None and user != None:
            factory = DBConnect.getConnection()
            idDAO = factory(classname = "Requestor.ID")
            userId = idDAO.execute(user)
            assocDAO = factory(classname = "Requestor.GetAssociation")
            assoc = assocDAO.execute(userId)
            if len(assoc) == 0:
                GroupManagement.addUserToGroup(user, group)
        if team != None:
            if not ProdManagement.teamExist(team):
                ProdManagement.addTeam(team)
                result['team'] = '%s registered' %team
            else:
                result['team'] = '%s already registered' %team
        else:
            raise cherrypy.HTTPError(400, "Bad input Team not defined")

        return result
