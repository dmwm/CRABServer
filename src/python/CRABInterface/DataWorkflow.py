import time
import threading
import logging
import cherrypy #cherrypy import is needed here because we need the 'start_thread' subscription

# WMCore dependecies here
from WMCore.REST.Error import ExecutionError, InvalidParameter
import WMCore.RequestManager.RequestMaker.Processing.AnalysisRequest #for registering Analysis request maker
from WMCore.Database.CMSCouch import CouchServer, CouchError, Database, CouchNotFoundError
from WMCore.RequestManager.RequestMaker.Registry import retrieveRequestMaker
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.RequestManager.RequestMaker import CheckIn
from WMCore.RequestManager.RequestDB.Interface.Request import ChangeState
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
import WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools as ReqMgrUtilities
from WMCore.Database.DBFactory import DBFactory
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx

#CRAB dependencies
from CRABInterface.DataUser import DataUser
from CRABInterface.Utils import setProcessingVersion

class DataWorkflow(object): #Page needed for debug methods used by DBFactory. Uses cplog
    """Entity that allows to operate on workflow resources"""
    splitMap = {'LumiBased' : 'lumis_per_job', 'EventBased' : 'events_per_job', 'FileBased' : 'files_per_job'}

    @staticmethod
    def globalinit(monurl, monname, asomonurl, asomonname, reqmgrurl, reqmgrname,
                   configcacheurl, configcachename, connectUrl, phedexargs=None,
                   sitewildcards={'T1*': 'T1_*', 'T2*': 'T2_*', 'T3*': 'T3_*'}):
        DataWorkflow.couchdb = CouchServer(monurl)
        DataWorkflow.database = DataWorkflow.couchdb.connectDatabase(monname)

        #WMBSHelper need the reqmgr couchurl and database name
        DataWorkflow.reqmgrurl = reqmgrurl
        DataWorkflow.reqmgrname = reqmgrname
        DataWorkflow.configcacheurl = configcacheurl
        DataWorkflow.configcachename = configcachename

        DataWorkflow.asocouchdb = CouchServer(asomonurl)
        DataWorkflow.asodatabase = DataWorkflow.couchdb.connectDatabase(asomonname)

        DataWorkflow.connectUrl = connectUrl
        DataWorkflow.sitewildcards = sitewildcards

        DataWorkflow.phedex = PhEDEx(responseType='xml', dict=phedexargs)

    def __init__(self):
        self.logger = logging.getLogger("CRABLogger.DataWorkflow")
        self.user = DataUser()

        self._initCache(self.sitewildcards)

        self.dbi = DBFactory(self.logger, self.connectUrl).connect()
        cherrypy.engine.subscribe('start_thread', self.initThread)

    def initThread(self, thread_index):
        """
        The ReqMgr expects the DBI to be contained in the Thread
        """
        myThread = threading.currentThread()
        myThread.dbi = self.dbi

    def getWorkflow(self, wf):
        options = {"startkey": wf, "endkey": wf, 'reduce': True, 'descending': True}
        try:
            doc = self.database.document(id=wf)
        except CouchNotFoundError:
            return {}
        agentDoc = self.database.loadView("WMStats", "latestRequest", options)
        if agentDoc['rows']:
            agentDoc = self.database.document(id=agentDoc['rows'][0]['value']['id'])
            doc['status'] = agentDoc['status']
            doc['sites'] = agentDoc['sites']
            return doc
        else:
            return doc

    def _initCache(self, sitewildcards):
        """Building the cache for frequently used information.
           This shouldn't be abused and should be refreshed sometimes.

           :arg dict sitewildcards: a dictionary containing site wildcards"""
        # caching site db sites with wildcards
        self.wildcardKeys = sitewildcards
        self.wildcardSites = {}
        self.allCMSNames = SiteDBJSON().getAllCMSNames()
        ReqMgrUtilities.addSiteWildcards(self.wildcardKeys, self.allCMSNames, self.wildcardSites)
        #anything else to be cached?

    def getAll(self, wfs):
        """Retrieves the workflow document from the couch database

           :arg str list workflow: a list of workflow names
           :return: a json corresponding to the workflow in couch"""

        for wf in wfs:
            yield self.getWorkflow(wf)

    def getLatests(self, user, limit, timestamp):
        """Retrives the latest workflows for the user

           :arg str user: a valid user hn login name
           :arg int limit: the maximum number of workflows to return
                          (this should probably have a default!)
           :arg int limit: limit on the workflow age
           :return: a list of workflows"""
        # convert the workflow age in something eatable by a couch view
        # in practice it's convenient that the timestamp is on a fixed format: latest 1 or 3 days, latest 1 week, latest 1 month
        # and that it's a list (probably it can be converted into it): [year, month-num, day, hh, mm, ss]
        # this will allow to query as it's described here: http://guide.couchdb.org/draft/views.html#many

        # example:
        # return self.database.loadView('WMStats', 'byUser',
        #                              options = { "startkey": user,
        #                                          "endkey": user,
        #                                          "limit": limit, })
        #raise NotImplementedError
        return [{}]

    def errors(self, workflow, shortformat):
        """Retrieves the sets of errors for a specific workflow

           :arg str workflow: a workflow name
           :arg int shortformat: a flag indicating if the user is asking for detailed
                                 information about sites and list of errors
           :return: a list of errors grouped by exit code, error reason, site"""

        for wf in workflow:
            group_level = 3 if shortformat else 5
            options = {"startkey": [wf, "jobfailed"], "endkey": [wf, "jobfailed", {}, {}, {}], "reduce": True,  "group_level": group_level}
            yield self.database.loadView("WMStats", "jobsByStatusWorkflow", options)['rows']

        yield [{}]

    def report(self, workflow):
        """Retrieves the quality of the workflow in term of what has been processed
           (eg: good lumis)

           :arg str workflow: a workflow name
           :return: what?"""

        # example:
        # return self.database.loadView('WMStats', 'getlumis',
        #                              options = { "startkey": workflow,
        #                                          "endkey": workflow,})
        raise NotImplementedError
        return [{}]

    def logs(self, workflow, howmany):
        """Returns the workflow logs PFN. It takes care of the LFN - PFN conversion too.

           :arg str workflow: a workflow name
           :arg int howmany: the limit on the number of PFN to return
           :return: (a generator of?) a list of logs pfns"""

        # example:
        # return self.database.loadView('WMStats', 'getlogs',
        #                              options = { "startkey": workflow,
        #                                          "endkey": workflow,
        #                                          "limit": howmany,})
        raise NotImplementedError
        return [{}]

    def output(self, workflows, howmany):
        """Returns the workflow output PFN. It takes care of the LFN - PFN conversion too.

           :arg str list workflow: a workflow name
           :arg int howmany: the limit on the number of PFN to return
           :return: a generator of list of output pfns"""

        options = {"reduce": False}
        # default 1
        # no limits if -1
        howmany = howmany if howmany else 1
        if howmany > 0:
            options["limit"] = howmany

        # retrieving from async stage out
        result = []
        for wf in workflows:
            # retrieves from async stage out
            options["startkey"] = [wf]
            options["endkey"] = [wf, {}]
            result += self.asodatabase.loadView("UserMonitoring", "FilesByWorkflow", options)['rows']
            # check if we have got enough and how many missing
            if 'limit' in options:
                if len(result) >= howmany:
                    break
                elif len(result) + options['limit'] > howmany:
                    options['limit'] = howmany - len(result)

        jobids = [singlefile['value']['jobid'] for singlefile in result]

        # retrieve from request monitoring couchdb?
        if 'limit' in options and len(result) < howmany:
            # retrieve from jobsummary
            options = {"reduce": False}
            for wf in workflows:
                options["startkey"] = [wf, 'output']
                options["endkey"] = [wf, 'output', {}]
                tempresult = self.database.loadView("WMStats", "filesByWorkflow", options)['rows']
                # merge the aso mon result and req mon result avoiding duplicated outputs
                tempresult = [t for t in tempresult if not t['value']['jobid'] in jobids]
                if len(tempresult) + len(result) > howmany:
                    result += tempresult[: howmany-len(result)]
                else:
                    result += tempresult
            """
            # this version doesn't work as expected, but is an example on avoiding full view load in rest memory
            if howmany:
                options["limit"] = howmany - len(result)
            for wf in workflows:
                options["startkey"] = [wf, 'output']
                options["endkey"] = [wf, 'output', {}]
                result += self.database.loadView("WMStats", "filesByWorkflow", options)['rows']
                # check if we have got enough and how many missing
                if 'limit' in options:
                    if len(result) >= howmany:
                        break
                    elif len(result) + options['limit'] > howmany:
                        options['limit'] = howmany - len(result)
            """
        for singlefile in result:
            singlefile['value']['workflow'] = singlefile['key'][0]
            singlefile['value']['pfn'] = self.phedex.getPFN(singlefile['value']['location'],
                                                            singlefile['value']['lfn'])[(singlefile['value']['location'],
                                                                                         singlefile['value']['lfn'])]
            del singlefile['value']['jobid']
            del singlefile['value']['location']
            del singlefile['value']['lfn']
            yield singlefile['value']

    def schema(self, workflow):
        """Returns the workflow schema parameters.

           :arg str workflow: a workflow name
           :return: a json corresponding to the workflow schema"""
        # it probably needs to connect to the reqmgr couch database
        # TODO: verify + code the above point
        # probably we need to explicitely select the schema parameters to return
        raise NotImplementedError
        return [{}]

    def configcache(self, workflow):
        """Returns the config cache associated to the workflow.

           :arg str workflow: a workflow name
           :return: the config cache couch json object"""
        # it probably needs to connect to the reqmgr and config cache couch databases
        # TODO: verify + code the above point
        raise NotImplementedError
        return [{}]

    def publish(self, workflow, dbsurl):
        """Perform the data publication of the workflow result.

           :arg str workflow: a workflow name
           :arg str dbsurl: the DBS URL endpoint where to publish
           :return: the publication status or result"""
        raise NotImplementedError
        return [{}]

    def _inject(self, request):
        # Auto Assign the requests
        ### what is the meaning of the Team in the Analysis use case?
        try:
            CheckIn.checkIn(request)
            ChangeState.changeRequestStatus(request['RequestName'], 'assignment-approved')
            ChangeState.assignRequest(request['RequestName'], request["Team"])
        #Raised during the check in
        except CheckIn.RequestCheckInError, re:
            self.logger.exception(re)
            raise ExecutionError("Problem checking in the request", errobj = re)
        #Raised by the change state
        except RuntimeError, re:
            self.logger.exception(re)
            raise ExecutionError("Problem checking in the request", errobj = re)

    def submit(self, workflow, jobtype, jobsw, jobarch, inputdata, siteblacklist, sitewhitelist, blockwhitelist,
               blockblacklist, splitalgo, algoargs, configdoc, userisburl, adduserfiles, addoutputfiles, savelogsflag,
               userdn, userhn, publishname, asyncdest, campaign, blacklistT1):
        """Perform the workflow injection into the reqmgr + couch

           :arg str workflow: workflow name requested by the user;
           :arg str jobtype: job type of the workflow, usually Analysis;
           :arg str jobsw: software requirement;
           :arg str jobarch: software architecture (=SCRAM_ARCH);
           :arg str list inputdata: input datasets;
           :arg str list siteblacklist: black list of sites, with CMS name;
           :arg str list sitewhitelist: white list of sites, with CMS name;
           :arg str asyncdest: CMS site name for storage destination of the output files;
           :arg str list blockwhitelist: selective list of input iblock from the specified input dataset;
           :arg str list blockblacklist:  input blocks to be excluded from the specified input dataset;
           :arg str splitalgo: algorithm to be used for the workflow splitting;
           :arg str algoargs: argument to be used by the splitting algorithm;
           :arg str configdoc: URL of the configuration object ot be used;
           :arg str userisburl: URL of the input sandbox file;
           :arg str list adduserfiles: list of additional input files;
           :arg str list addoutputfiles: list of additional output files;
           :arg int savelogsflag: archive the log files? 0 no, everything else yes;
           :arg str publishname: name to use for data publication;
           :arg str asyncdest: final destination of workflow output files;
           :arg str campaign: needed just in case the workflow has to be appended to an existing campaign;
           :returns: a dict which contaians details of the request"""

        #add the user in the reqmgr database
        self.user.addNewUser(userdn, userhn)
        requestname = '%s_%s_%s' % (userhn, workflow, time.strftime('%y%m%d_%H%M%S', time.gmtime()))

        schemaWf = { "CouchUrl": self.configcacheurl,
                     "CouchDBName": self.configcachename,
                     "AnalysisConfigCacheDoc": configdoc,
                     "RequestName": requestname,
                     "OriginalRequestName": workflow, # do we really need this?
                     "SiteWhitelist": sitewhitelist,
                     "SiteBlacklist": siteblacklist,
                     "CMSSWVersion": jobsw,
                     "RequestorDN": userdn,
                     "SaveLogs": bool(savelogsflag),
                     "InputDataset": inputdata,
                     "OutputFiles": addoutputfiles,
                     "Group": "Analysis",
                     "Team": "Analysis",
                     "RequestType": jobtype,
                     "userFiles": adduserfiles,
                     "ScramArch": jobarch,
                     "JobSplitAlgo": splitalgo,
                     "userSandbox": userisburl,
                     "PublishDataName": publishname,
                     "asyncDest": asyncdest,
                     "JobSplitArgs": { self.splitMap[splitalgo] : algoargs },
                     "Campaign": campaign or requestname, # for first submissions this should be = to the wf name
                     "Submission": 1, # easy to track the relation between resubmissions,
                     "Requestor" : userhn,
                     "Username"  : userhn,
                   }

        if not asyncdest in self.allCMSNames:
            excasync = ValueError("The parameter asyncdest %s is not in the list of known CMS sites %s" % (asyncdest, self.allCMSNames))
            raise InvalidParameter("Remote output data site not valid", errobj = excasync)

        #TODO where's the ACDC?
        #requestSchema["ACDCUrl"] =  self.ACDCCouchURL
        #requestSchema["ACDCDBName"] =  self.ACDCCouchDB
        #TODO is it needed?
        #requestSchema['OriginalRequestName'] = requestSchema['RequestName']

        schemaWf["ProcessingVersion"] = setProcessingVersion(schemaWf, self.reqmgrurl, self.reqmgrname)

        maker = retrieveRequestMaker("Analysis")
        specificSchema = maker.schemaClass()
        specificSchema.update(schemaWf)
#       TODO do we really need these three instructions? At the end url is (from the old reqmgr) http://crabas.lnl.infn.it:8188/crabinterface/crab
#        url = cherrypy.url()
        # we only want the first part, before /task/
#        url = url[0:url.find('/task')]
#        specificSchema.reqMgrURL = url

        if schemaWf.get("ACDCDoc", None) and schemaWf['JobSplitAlgo'] != 'LumiBased':
            raise InvalidParameter('You must use LumiBased splitting if specifying a lumiMask.')

        try:
            specificSchema.allCMSNames = self.allCMSNames
            specificSchema.validate()
        except Exception, ex:
            raise InvalidParameter("Not valid scehma provided", errobj = ex)

        #The client set BlacklistT1 as true if the user has not t1access role.
        if blacklistT1:
            if schemaWf['SiteBlacklist']:
                schemaWf['SiteBlacklist'].append("T1*")
            else:
                specificSchema['SiteBlacklist'] = ["T1*"]

        request = maker(specificSchema)

        helper = WMWorkloadHelper(request['WorkflowSpec'])

        # can't save Request object directly, because it makes it hard to retrieve the _rev
        metadata = {}
        metadata.update(request)
        # don't want to JSONify the whole workflow
        del metadata['WorkflowSpec']
        helper.setSiteWildcardsLists(siteWhitelist = specificSchema.get("SiteWhitelist",[]), siteBlacklist = specificSchema.get("SiteBlacklist",[]),
                                     wildcardDict = self.wildcardSites)
        request['RequestWorkflow'] = helper.saveCouch(self.reqmgrurl, self.reqmgrname, metadata=metadata)
        request['PrepID'] = None

        self._inject(request)

        return [{'RequestName': request['RequestName']}]

    def resubmit(self, workflow):
        """Request to reprocess what the workflow hasn't finished to reprocess.
           This needs to create a new workflow in the same campaign"""
        # TODO: part of the code here needs to be shared with inject
        raise NotImplementedError
        return [{}]

    def kill(self, workflow, force):
        """Request to Abort a workflow.

           :arg str workflow: a workflow name
           :arg int force: force to delete the workflows in any case; 0 no, everything else yes
           :return: the operation result"""
        raise NotImplementedError
        return [{}]
