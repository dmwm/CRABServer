import time
import threading
import logging
import cherrypy #cherrypy import is needed here because we need the 'start_thread' subscription
import traceback

# WMCore dependecies here
from WMCore.REST.Error import ExecutionError, InvalidParameter
import WMCore.RequestManager.RequestMaker.Processing.AnalysisRequest #for registering Analysis request maker
import WMCore.RequestManager.RequestMaker.Production.PrivateMCRequest
from WMCore.Database.CMSCouch import CouchServer, CouchError, Database, CouchNotFoundError
from WMCore.RequestManager.RequestMaker.Registry import retrieveRequestMaker
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.RequestManager.RequestMaker import CheckIn
from WMCore.RequestManager.RequestDB.Interface.Request import ChangeState, GetRequest
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import loadWorkload, abortRequest
from WMCore.Database.DBFactory import DBFactory

#CRAB dependencies
from CRABInterface.DataUser import DataUser
from CRABInterface.Utils import setProcessingVersion, CouchDBConn, CMSSitesCache, conn_handler
from CRABInterface.Regexps import RX_WFRESUB

class DataWorkflow(object):
    """Entity that allows to operate on workflow resources.
       No aggregation of workflows provided here."""
    splitMap = {'LumiBased' : 'lumis_per_job', 'EventBased' : 'events_per_job', 'FileBased' : 'files_per_job'}

    @staticmethod
    def globalinit(monurl, monname, asomonurl, asomonname, reqmgrurl, reqmgrname,
                   configcacheurl, configcachename, connectUrl, phedexargs=None,
                   dbsurl=None, acdcurl=None, acdcdb=None,
                   sitewildcards={'T1*': 'T1_*', 'T2*': 'T2_*', 'T3*': 'T3_*'}):

        DataWorkflow.monitordb = CouchDBConn(db=CouchServer(monurl), name=monname, conn=None)
        DataWorkflow.asodb = CouchDBConn(db=CouchServer(asomonurl), name=asomonname, conn=None)

        DataWorkflow.wmstatsurl = monurl + '/' + monname
        DataWorkflow.wmstats = None

        #WMBSHelper need the reqmgr couchurl and database name
        DataWorkflow.reqmgrurl = reqmgrurl
        DataWorkflow.reqmgrname = reqmgrname
        DataWorkflow.configcacheurl = configcacheurl
        DataWorkflow.configcachename = configcachename

        DataWorkflow.connectUrl = connectUrl
        DataWorkflow.sitewildcards = sitewildcards

        DataWorkflow.phedexargs = phedexargs
        DataWorkflow.phedex = None

        DataWorkflow.dbsurl = dbsurl

        DataWorkflow.acdcurl = acdcurl
        DataWorkflow.acdcdb = acdcdb

    def __init__(self):
        self.logger = logging.getLogger("CRABLogger.DataWorkflow")
        self.user = DataUser()

        self.wildcardKeys = self.sitewildcards
        self.wildcardSites = {}
        self.allCMSNames = CMSSitesCache(cachetime=0, sites={})

        self.dbi = DBFactory(self.logger, self.connectUrl).connect()
        cherrypy.engine.subscribe('start_thread', self.initThread)

        # Supporting different types of workflows means providing
        # different functionalities depending on the type.
        # This dictionary contains pointer to workflow type methods
        # (when many more methods are needed it can be evaluated to have
        #  a common base class and implemantation of types with common
        #  naming convention).
        self.typemapping = {'Analysis': {'report': self._reportAnalysis},
                            'PrivateMC': {'report': self._reportPrivateMC},}

    def initThread(self, thread_index):
        """
        The ReqMgr expects the DBI to be contained in the Thread
        """
        myThread = threading.currentThread()
        myThread.dbi = self.dbi

    @conn_handler(services=['monitor'])
    def getWorkflow(self, wf):
        """
        Get the workflow summary documents frmo the Central Monitoring.

        :arg str wf: the workflow name
        :return: the summary documents in a dictionary.
        """
        options = {"startkey": [wf, {}], "endkey": [wf], 'reduce': True, 'descending': True}
        try:
            doc = self.monitordb.conn.document(id=wf)
        except CouchNotFoundError:
            self.logger.error("Cannot find document with id " + str(id))
            return {}
        agentDoc = self.monitordb.conn.loadView("WMStats", "latestRequest", options)
        if agentDoc['rows']:
            agentDoc = self.monitordb.conn.document(id=agentDoc['rows'][0]['value']['id'])
            doc['status'] = agentDoc['status']
            doc['sites'] = agentDoc['sites']
            return doc
        else:
            return doc

    @conn_handler(services=['asomonitor'])
    def getWorkflowTransfers(self, wf):
        """
        Return the async transfers concerning the workflow

        :arg str wf: the workflow name
        :return: the summary document in a dictionary.
        """
        try:
            doc = self.asodb.conn.document(wf)
            self.logger.debug("Workflow trasfer: %s" % doc)
            if doc and 'state' in doc:
                 return doc['state']
        except CouchNotFoundError:
            self.logger.error("Cannot find ASO documents in couch")
        return {}

    def getAll(self, wfs):
        """Retrieves the workflow document from the couch database

           :arg str list workflow: a list of workflow names
           :return: a json corresponding to the workflow in couch"""
        for wf in wfs:
            yield self.getWorkflow(wf)

    @conn_handler(services=['monitor'])
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
        # return self.monitordb.conn.loadView('WMStats', 'byUser',
        #                              options = { "startkey": user,
        #                                          "endkey": user,
        #                                          "limit": limit, })
        #raise NotImplementedError
        return [{}]

    @conn_handler(services=['monitor'])
    def errors(self, workflow, shortformat):
        """Retrieves the sets of errors for a specific workflow

           :arg str workflow: a workflow name
           :arg int shortformat: a flag indicating if the user is asking for detailed
                                 information about sites and list of errors
           :return: a list of errors grouped by exit code, error reason, site"""

        group_level = 3 if shortformat else 5
        options = {"startkey": [workflow, "jobfailed"], "endkey": [workflow, "jobfailed", {}, {}, {}], "reduce": True,  "group_level": group_level}
        return self.monitordb.conn.loadView("WMStats", "jobsByStatusWorkflow", options)['rows']

    def report(self, workflow):
        """Retrieves the quality of the workflow in term of what has been processed
           (eg: good lumis)

           :arg str workflow: a workflow name
           :return: what?"""

        try:
            return self.typemapping[self.getType(workflow)]['report'](workflow)
        except KeyError, ex:
            raise InvalidParameter("Not valid scehma provided", trace=traceback.format_exc(), errobj = ex)

    @conn_handler(services=['monitor', 'phedex'])
    def logs(self, workflow, howmany, exitcode):
        """Returns the workflow logs PFN. It takes care of the LFN - PFN conversion too.

           :arg str workflow: a workflow name
           :arg int howmany: the limit on the number of PFN to return
           :arg int exitcode: the log has to be of a job ended with this exit_code
           :return: (a generator of?) a list of logs pfns"""
        #default is 1 logfile per exitcode
        howmany = howmany if howmany else 1
        self.logger.info("Retrieving %s log(s) for %s" % (howmany, workflow))

        #number of jobs for each exit code
        numJobs = {}
        #number of jobs with no logs for each exit code
        missLogs = {}

        ## TODO optimize this by avoiding loading full view in memory
        options = {"reduce": False, "include_docs" : True}
        if exitcode == None:
            options["startkey"] = [workflow]
            options["endkey"] = [workflow, {}, {}, {}, {}]
        elif exitcode != 0:
            options["startkey"] = [workflow, "jobfailed", exitcode]
            options["endkey"] = [workflow, "jobfailed", exitcode, {}, {}]
        else:
            options["startkey"] = [workflow, "success", exitcode]
            options["endkey"] = [workflow, "success", exitcode, {}, {}]
        outview = self.monitordb.conn.loadView("WMStats", "jobsByStatusWorkflow", options)['rows']

        for row in outview:
            elem = {}
            #if the fwjr does not have outputs
            if not 'output' in row['doc'] or not row['doc']['output']:
                if not row['doc']['task'].endswith("/LogCollect"):
                    missLogs[str(row['doc']['exitcode'])] = 1 if not str(row['doc']['exitcode']) in missLogs else missLogs[str(row['doc']['exitcode'])] + 1
            elif row['doc']['task'].endswith("/LogCollect"):
                for output in row['doc']['output']:
                    if 'lfn' in output:
                        elem["type"] = "logCollect"
                        elem["size"] = output['size']
                        elem["checksums"] = output['checksums']
                        #Note: in this report we have the pfn and not the lfn
                        elem["pfn"] = output['lfn']
                        elem["workflow"] = workflow
            elif row['doc']['exitcode'] in numJobs and numJobs[row['doc']['exitcode']] >= howmany:
                if exitcode is not None:
                    break
                else:
                    continue
            else: #a real job
                found = False
                for output in row['doc']['output']:
                    #when we find the logarchive output we fill elem
                    if output['type'] == 'logArchive':
                        elem["type"] = "logArchive"
                        elem["size"] = output['size']
                        elem["checksums"] = output['checksums']
                        elem["pfn"] = self.phedex.getPFN(row['doc']['site'],
                                                         output['lfn'])[(row['doc']['site'], output['lfn'])]
                        elem["workflow"] = workflow
                        elem["exitcode"] = row['doc']['exitcode']
                        numJobs[row['doc']['exitcode']] = 1 if not row['doc']['exitcode'] in numJobs else numJobs[row['doc']['exitcode']] + 1
                        found = True
                if not found:
                    missLogs[str(row['doc']['exitcode'])] = 1 if not str(row['doc']['exitcode']) in missLogs else missLogs[str(row['doc']['exitcode'])] + 1

            if elem:
                yield elem
        # how many job report did not have an exit code are reported in missing
        yield {"missing": missLogs}

    @conn_handler(services=['asomonitor'])
    def outputLocation(self, workflow, max):
        """
        Retrieves the output LFN from async stage out

        :arg str workflow: the unique workflow name
        :arg int max: the maximum number of output files to retrieve
        :return: the result of the view as it is."""
        options = {"reduce": False, "startkey": [workflow], "endkey": [workflow, {}]}
        if max:
            options["limit"] = max
        return self.asodb.conn.loadView("UserMonitoring", "FilesByWorkflow", options)['rows']

    @conn_handler(services=['monitor'])
    def outputTempLocation(self, workflow, howmany, jobtoskip):
        """Returns the workflow output LFN from the temporary location after the
           local stage out at the site where the job was running.

           :arg str workflow: a workflow name
           :arg int howmany: the limit on the number of PFN to return
           :arg list int: the list of jobid to ignore
           :return: a generator of list of output pfns"""
        options = {"reduce": False, "limit": howmany}
        options["startkey"] = [workflow, 'output']
        options["endkey"] = [workflow, 'output', {}]
        tempresult = self.monitordb.conn.loadView("WMStats", "filesByWorkflow", options)['rows']
        return [t for t in tempresult if not t['value']['jobid'] in jobtoskip]

    @conn_handler(services=['phedex'])
    def getPhyisicalLocation(self, result):
        """
        Translates LFN to PFN

        :arg dict list result: the list of dictionaties containing the file information
        :return: a generator of dictionaries reporting the file information in PFN."""
        for singlefile in result:
            singlefile['value']['workflow'] = singlefile['key'][0]
            singlefile['value']['pfn'] = self.phedex.getPFN(singlefile['value']['location'],
                                                            singlefile['value']['lfn'])[(singlefile['value']['location'],
                                                                                         singlefile['value']['lfn'])]
            del singlefile['value']['jobid']
            del singlefile['value']['location']
            del singlefile['value']['lfn']
            yield singlefile['value']

    def output(self, workflow, howmany):
        """
        Retrieves the output PFN aggregating output in final and temporary locations.

        :arg str workflow: the unique workflow name
           :arg int howmany: the limit on the number of PFN to return
           :return: a generator of list of outputs"""
        howmany = howmany if howmany else 1
        max = howmany if howmany > 0 else None
        result = self.workflow.outputLocation(wf, max)

        if not max or len(result) < howmany:
            jobids = [singlefile['value']['jobid'] for singlefile in result]
            tempresult = self.workflow.outputTempLocation(wf, howmany-len(result), jobids)
            if len(tempresult) + len(result) >= howmany:
                result += tempresult[: howmany-len(result)]

        return self.getPhyisicalLocation(result)

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

    @conn_handler(services=['wmstats'])
    def _inject(self, request):
        """
        Injecting and auto assigning the requests inside ReqMgr.

        :arg dict request: the request schema to inject"""
        try:
            CheckIn.checkIn(request, wmstatSvc=self.wmstats)
        except CheckIn.RequestCheckInError, re:
            raise ExecutionError("Problem checking in the request: %s" % getattr(re, '_message', 'unknown'), trace=traceback.format_exc(), errobj=re)
        try:
            ChangeState.changeRequestStatus(request['RequestName'], 'assignment-approved', wmstatUrl=self.wmstatsurl)
            ChangeState.assignRequest(request['RequestName'], request["Team"], wmstatUrl=self.wmstatsurl)
        except RuntimeError, re:
            raise ExecutionError("Problem checking in the request", trace=traceback.format_exc(), errobj=re)


    def _injectCouch(self, schemaWf, makerType):
        maker = retrieveRequestMaker(makerType)
        specificSchema = maker.newSchema()
        specificSchema.update(schemaWf)
        try:
            specificSchema.allCMSNames = self.allCMSNames.sites
            specificSchema.validate()
        except Exception as ex:
            raise InvalidParameter("Not valid scehma provided", trace=traceback.format_exc(), errobj=ex)
        #The client set BlacklistT1 as true if the user has not t1access role.
        if specificSchema["BlacklistT1"]:
            if specificSchema['SiteBlacklist']:
                specificSchema['SiteBlacklist'].append("T1*")
            else:
                specificSchema['SiteBlacklist'] = ["T1*"]
        request = maker(specificSchema)
        helper = WMWorkloadHelper(request['WorkflowSpec'])
        # can't save Request object directly, because it makes it hard to retrieve the _rev
        metadata = {}
        metadata.update(request) # don't want to JSONify the whole workflow
        del metadata['WorkflowSpec']
        siteBlacklist = specificSchema.get("SiteBlacklist", [])
        siteWhitelist = specificSchema.get("SiteWhitelist", [])
        helper.setSiteWildcardsLists(siteWhitelist=siteWhitelist, siteBlacklist=siteBlacklist, wildcardDict=self.wildcardSites)
        helper.setDashboardActivity(dashboardActivity = 'Analysis')
        if not set(siteBlacklist).isdisjoint( set(siteWhitelist) ):
            raise InvalidParameter("SiteBlacklist and SiteWhitelist are not disjoint: %s in both lists" % \
                                   (','.join( set(siteBlacklist) & set(siteWhitelist) )))
        request['RequestWorkflow'] = helper.saveCouch(self.reqmgrurl, self.reqmgrname, metadata=metadata)
        return request


    @conn_handler(services=['sitedb'])
    def submit(self, workflow, jobtype, jobsw, jobarch, inputdata, siteblacklist, sitewhitelist, blockwhitelist,
               blockblacklist, splitalgo, algoargs, configdoc, userisburl, adduserfiles, addoutputfiles, savelogsflag,
               userdn, userhn, publishname, asyncdest, campaign, blacklistT1, dbsurl, publishdbsurl, acdcdoc):
        """Perform the workflow injection into the reqmgr + couch

           :arg str workflow: workflow name requested by the user;
           :arg str jobtype: job type of the workflow, usually Analysis;
           :arg str jobsw: software requirement;
           :arg str jobarch: software architecture (=SCRAM_ARCH);
           :arg str inputdata: input dataset;
           :arg str list siteblacklist: black list of sites, with CMS name;
           :arg str list sitewhitelist: white list of sites, with CMS name;
           :arg str list blockwhitelist: selective list of input iblock from the specified input dataset;
           :arg str list blockblacklist:  input blocks to be excluded from the specified input dataset;
           :arg str splitalgo: algorithm to be used for the workflow splitting;
           :arg str algoargs: argument to be used by the splitting algorithm;
           :arg str configdoc: URL of the configuration object ot be used;
           :arg str userisburl: URL of the input sandbox file;
           :arg str list adduserfiles: list of additional input files;
           :arg str list addoutputfiles: list of additional output files;
           :arg int savelogsflag: archive the log files? 0 no, everything else yes;
           :arg str userdn: DN of user doing the request;
           :arg str userhn: hyper new name of the user doing the request;
           :arg str publishname: name to use for data publication;
           :arg str asyncdest: CMS site name for storage destination of the output files;
           :arg str campaign: needed just in case the workflow has to be appended to an existing campaign;
           :arg int blacklistT1: flag enabling or disabling the black listing of Tier-1 sites;
           :arg str dbsurl: dbs url where the input dataset is published;
           :arg str publishdbsurl: dbs url where the output data has to be published;
           :returns: a dict which contaians details of the request"""

        #add the user in the reqmgr database
        self.user.addNewUser(userdn, userhn)
        requestname = '%s_%s_%s' % (userhn, workflow, time.strftime('%y%m%d_%H%M%S', time.gmtime()))

        schemaWf = { "CouchURL": self.configcacheurl,
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
                     "Requestor": userhn,
                     "Username": userhn,
                     "BlacklistT1": blacklistT1,
                     "DbsUrl": dbsurl if dbsurl is not None else self.dbsurl,
                     "PublishDbsUrl": publishdbsurl,
                     "ACDCUrl": self.acdcurl,
                     "ACDCDBName": self.acdcdb,
                     "ACDCDoc": acdcdoc,
                   }

        if not asyncdest in self.allCMSNames.sites:
            excasync = ValueError("The parameter asyncdest %s is not in the list of known CMS sites %s" % (asyncdest, self.allCMSNames.sites))
            invalidp = InvalidParameter("Remote output data site not valid", errobj = excasync)
            setattr(invalidp, 'trace', '')
            raise invalidp
        if schemaWf.get("ACDCDoc", None) and schemaWf['JobSplitAlgo'] != 'LumiBased':
            excsplit = ValueError("You must use LumiBased splitting if specifying a lumiMask.")
            invalidp = InvalidParameter("You must use LumiBased splitting if specifying a lumiMask.", errobj = excsplit)
            setattr(invalidp, 'trace', '')
            raise invalidp

        schemaWf["ProcessingVersion"] = setProcessingVersion(schemaWf, self.reqmgrurl, self.reqmgrname)

        makerType = "Analysis"
        request = self._injectCouch(schemaWf, makerType)
        request['PrepID'] = None

        self._inject(request)

        return [{'RequestName': request['RequestName']}]


    @conn_handler(services=['sitedb'])
    def resubmit(self, workflow, siteblacklist, sitewhitelist):
        """Request to reprocess what the workflow hasn't finished to reprocess.
           This needs to create a new workflow in the same campaign

           :arg str workflow: a valid workflow name
           :arg str list siteblacklist: black list of sites, with CMS name;
           :arg str list sitewhitelist: white list of sites, with CMS name."""
        # TODO: change this
        taskresubmit = "Analysis"
        originalwf = GetRequest.getRequestByName( workflow )
        helper = loadWorkload(originalwf)
        originalschema = helper.data.request.schema.dictionary_()
        wmtask = helper.getTask(taskresubmit)
        if not wmtask:
            exctask = ValueError('%s task not found in the workflow %s.' %(taskResubmit, lastSubmission))
            invalidp = InvalidParameter("Problem resubmitting workflow because task to resubmit was not found.", errobj = exctask)
            setattr(invalidp, 'trace', '')
            raise invalidp
        taskpath = wmtask.getPathName()
        resubmitschema = originalschema.copy()
        newpars = { 'OriginalRequestName': workflow,
                    'InitialTaskPath':     taskpath,
                    'SiteWhitelist':       sitewhitelist if sitewhitelist else originalschema.get('SiteWhitelist', []),
                    'SiteBlacklist':       siteblacklist if siteblacklist else originalschema.get('SiteBlacklist', []),
                    'Submission':          originalschema['Submission'] + 1,
                    'ACDCServer':          originalschema['ACDCUrl'],
                    'ACDCDatabase':        originalschema['ACDCDBName'],
                  }

        ## generating a new name: I am choosing the original name plus resubmit and date
        if RX_WFRESUB.match(workflow):
            times = int(workflow.rsplit('_resubmit')[-1].split('_')[-1])+1
            basenewname = workflow.rsplit('_resubmit')[0]
            newpars['RequestName'] = basenewname + '_resubmit_' + str(times)
        else:
            newpars['RequestName'] = workflow + '_resubmit_1'

        resubmitschema.update(newpars)
        request = self._injectCouch(resubmitschema, "Resubmission")
        self._inject(request)

    def status(self, workflow):
        """Retrieve the status of the workflow

           :arg str workflow: a valid workflow name
           :return: a workflow status summary document"""
        yield self.getWorkflow(wf)

    def kill(self, workflow):
        """Request to Abort a workflow.

           :arg str workflow: a workflow name"""
        try:
            ChangeState.changeRequestStatus(workflow, 'aborted', wmstatUrl=self.wmstatsurl)
            abortRequest(workflow)
        except RuntimeError, re:
            raise ExecutionError("Problem killing the request", trace=traceback.format_exc(), errobj=re)

    @conn_handler(services=['monitor'])
    def getType(self, workflow):
        """Retrieves the workflow type from the monitoring

           :arg str workflow: a workflow name
           :return: a string of the job type supported by the workflow."""
        return self.monitordb.conn.document(id=workflow).get('request_type', None)

    @conn_handler(services=['monitor'])
    def _reportAnalysis(self, workflow):
        """Retrieves the quality of the workflow in term of what has been processed
           from an analysis point of view, looking at good lumis.

           :arg str workflow: a workflow name
           :return: dictionary with run-lumis information"""

        options = {"reduce": False, "include_docs" : True, "startkey": [workflow, "success", 0], "endkey": [workflow, "success", 0, {}, {}]}
        output = {}
        for singlelumi in self.monitordb.conn.loadView("WMStats", "jobsByStatusWorkflow", options)['rows']:
            if 'lumis' in singlelumi['doc'] and singlelumi['doc']['lumis']:
                for run in singlelumi['doc']['lumis']:
                    output.update((k, run[k]+output.get(k,[])) for k in run)
        return [output]

    def _reportPrivateMC(self, workflow):
        """Retrieves the quality of the workflow in term of what has been processed.

           :arg str workflow: a workflow name
           :return: what?"""

        raise NotImplementedError
