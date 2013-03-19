import time
import datetime
import threading
import logging
import cherrypy #cherrypy import is needed here because we need the 'start_thread' subscription
import traceback
import json

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
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
from WMCore.Database.DBFactory import DBFactory
from WMCore.WMSpec.WMTask import buildLumiMask

#CRAB dependencies
from CRABInterface.Utils import CMSSitesCache, conn_handler
from CRABInterface.Utils import retriveUserCert
from CRABInterface.Regexps import RX_WFRESUB

#SQL queries
from TaskDB.Oracle.Task.New import New
from TaskDB.Oracle.Task.ID import ID

class DataWorkflow(object):
    """Entity that allows to operate on workflow resources.
       No aggregation of workflows provided here."""
    splitMap = {'LumiBased' : 'lumis_per_job', 'EventBased' : 'events_per_job', 'FileBased' : 'files_per_job'}

    @staticmethod
    def globalinit(dbapi, phedexargs=None, dbsurl=None):
        DataWorkflow.api = dbapi
        DataWorkflow.phedexargs = phedexargs
        DataWorkflow.phedex = None
        DataWorkflow.dbsurl = dbsurl

    def __init__(self):
        self.logger = logging.getLogger("CRABLogger.DataWorkflow")
        self.allCMSNames = CMSSitesCache(cachetime=0, sites={})

        # Supporting different types of workflows means providing
        # different functionalities depending on the type.
        # This dictionary contains pointer to workflow type methods
        # (when many more methods are needed it can be evaluated to have
        #  a common base class and implemantation of types with common
        #  naming convention).
        self.typemapping = {'Analysis': {'report': self._reportAnalysis},
                            'PrivateMC': {'report': self._reportPrivateMC},}
        self.splitArgMap = { "LumiBased" : "lumis_per_job",
                        "FileBased" : "files_per_job"
                      }

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

    def errors(self, workflow, shortformat):
        """Retrieves the sets of errors for a specific workflow

           :arg str workflow: a workflow name
           :arg int shortformat: a flag indicating if the user is asking for detailed
                                 information about sites and list of errors
           :return: a list of errors grouped by exit code, error reason, site"""

        raise NotImplementedError

    def report(self, workflow):
        """Retrieves the quality of the workflow in term of what has been processed
           (eg: good lumis)

           :arg str workflow: a workflow name
           :return: what?"""

        raise NotImplementedError
        try:
            return self.typemapping[self.getType(workflow)]['report'](workflow)
        except KeyError, ex:
            raise InvalidParameter("Not valid scehma provided", trace=traceback.format_exc(), errobj = ex)

    def logs(self, workflow, howmany, exitcode, pandaids):
        """Returns the workflow logs PFN. It takes care of the LFN - PFN conversion too.

           :arg str workflow: a workflow name
           :arg int howmany: the limit on the number of PFN to return
           :arg int exitcode: the log has to be of a job ended with this exit_code
           :return: (a generator of?) a list of logs pfns"""
        #default is 1 logfile per exitcode
        self.logger.info("Retrieving %s log(s) for %s" % (howmany, workflow))
        raise NotImplementedError


    @conn_handler(services=['asomonitor'])
    def outputLocation(self, workflow, max, pandaids):
        """
        Retrieves the output LFN from async stage out

        :arg str workflow: the unique workflow name
        :arg int max: the maximum number of output files to retrieve
        :return: the result of the view as it is."""
        options = {"reduce": False, "startkey": [workflow, "output"], "endkey": [workflow, 'output', {}]}
        if max and not pandaids: #do not use limits if there are pandaids
            options["limit"] = max
        return self._filterids(self.asodb.conn.loadView("UserMonitoring", "FilesByWorkflow", options)['rows'], pandaids)

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

    def output(self, workflow, howmany):
        """
        Retrieves the output PFN aggregating output in final and temporary locations.

        :arg str workflow: the unique workflow name
           :arg int howmany: the limit on the number of PFN to return
           :return: a generator of list of outputs"""
        raise NotImplementedError
        self.phedex.getPFN(location, pfn)

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

    @retriveUserCert(clean=False)
    def submit(self, workflow, jobtype, jobsw, jobarch, inputdata, siteblacklist, sitewhitelist, blockwhitelist,
               blockblacklist, splitalgo, algoargs, configdoc, userisburl, cachefilename, cacheurl, adduserfiles, addoutputfiles, savelogsflag,
               userhn, publishname, asyncdest, campaign, blacklistT1, dbsurl, vorole, vogroup, publishdbsurl, tfileoutfiles, edmoutfiles, userdn,
               runs, lumis): #TODO delete unused parameters
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
           :arg str list runs: list of run numbers
           :arg str list lumis: list of lumi section numbers
           :returns: a dict which contaians details of the request"""

        self.logger.debug("""workflow %s, jobtype %s, jobsw %s, jobarch %s, inputdata %s, siteblacklist %s, sitewhitelist %s, blockwhitelist %s,
               blockblacklist %s, splitalgo %s, algoargs %s, configdoc %s, userisburl %s, cachefilename %s, cacheurl %s, adduserfiles %s, addoutputfiles %s, savelogsflag %s,
               userhn %s, publishname %s, asyncdest %s, campaign %s, blacklistT1 %s, dbsurl %s, publishdbsurl %s, tfileoutfiles %s, edmoutfiles %s, userdn %s,
               runs %s, lumis %s"""%(workflow, jobtype, jobsw, jobarch, inputdata, siteblacklist, sitewhitelist, blockwhitelist,\
               blockblacklist, splitalgo, algoargs, configdoc, userisburl, cachefilename, cacheurl, adduserfiles, addoutputfiles, savelogsflag,\
               userhn, publishname, asyncdest, campaign, blacklistT1, dbsurl, publishdbsurl, tfileoutfiles, edmoutfiles, userdn,\
               runs, lumis))
        #add the user in the reqmgr database
        timestamp = time.strftime('%y%m%d_%H%M%S', time.gmtime())
        requestname = '%s_%s_%s' % (timestamp, userhn, workflow)
        splitArgName = self.splitArgMap[splitalgo]
        dbSerializer = str

        self.api.modify(New.sql,
                            task_name       = [requestname],\
                            jobset_id       = [None],
                            task_status     = ['NEW'],\
                            start_time      = [datetime.datetime.now()],\
                            task_failure    = [''],\
                            job_sw          = [jobsw],\
                            job_arch        = [jobarch],\
                            input_dataset   = [inputdata],\
                            site_whitelist   = [json.dumps(sitewhitelist)],\
                            site_blacklist  = [json.dumps(siteblacklist)],\
                            split_algo      = [splitalgo],\
                            split_args      = [dbSerializer({'halt_job_on_file_boundaries': False, 'splitOnRun': False, splitArgName : algoargs})],\
                            #parameter userisburl is the same as cacheurl since we use panda
                            #and the pset is put in the pandacache with the usersandbox (and not in the configcache)
                            user_sandbox    = [cacheurl],\
                            cache_url       = [cacheurl],\
                            username        = [userhn],\
                            user_dn         = [userdn],\
                            user_vo         = ['cms'],\
                            user_role       = [''],\
                            user_group      = [''],\
                            publish_name    = [publishname],\
                            asyncdest       = [asyncdest],\
                            dbs_url         = [dbsurl],\
                            publish_dbs_url = [publishdbsurl],\
                            outfiles        = [dbSerializer(addoutputfiles)],\
                            tfile_outfiles  = [dbSerializer(tfileoutfiles)],\
                            edm_outfiles    = [dbSerializer(edmoutfiles)],\
                            data_runs       = [dbSerializer(buildLumiMask(runs, lumis))],\
                            transformation  = ['http://common-analysis-framework.cern.ch/CMSRunAnaly.sh'],\
                            arguments       = [''],\
        )

        """
        if schemaWf.get("ACDCDoc", None) and schemaWf['JobSplitAlgo'] != 'LumiBased':
            excsplit = ValueError("You must use LumiBased splitting if specifying a lumiMask.")
            invalidp = InvalidParameter("You must use LumiBased splitting if specifying a lumiMask.", errobj = excsplit)
            setattr(invalidp, 'trace', '')
            raise invalidp

        try:
            specificSchema.allCMSNames = self.allCMSNames.sites
            specificSchema.validate()
        except Exception, ex:
            raise InvalidParameter("Not valid scehma provided", trace=traceback.format_exc(), errobj = ex)

        #The client set BlacklistT1 as true if the user has not t1access role.
        if blacklistT1:
            if schemaWf['SiteBlacklist']:
                schemaWf['SiteBlacklist'].append("T1*")
            else:
                specificSchema['SiteBlacklist'] = ["T1*"]
        """

        return [{'RequestName': requestname}]

    def resubmit(self, workflow, siteblacklist, sitewhitelist):
        """Request to reprocess what the workflow hasn't finished to reprocess.
           This needs to create a new workflow in the same campaign

           :arg str workflow: a valid workflow name
           :arg str list siteblacklist: black list of sites, with CMS name;
           :arg str list sitewhitelist: white list of sites, with CMS name."""
        # TODO: change this
        taskresubmit = "Analysis"
        # TODO: part of the code here needs to be shared with inject
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
        resubmitschema.update(newpars)
        maker = retrieveRequestMaker("Resubmission")
        schema = maker.newSchema()
        ## updating it with resubmission parameters
        schema.update( resubmitschema )
        ## generating a new name: I am choosing the original name plus resubmit and date

        if RX_WFRESUB.match(workflow):
            times = int(workflow.rsplit('_resubmit')[-1].split('_')[-1])+1
            basenewname = workflow.rsplit('_resubmit')[0]
            schema['RequestName'] = basenewname + '_resubmit_' + str(times)
        else:
            schema['RequestName'] = workflow + '_resubmit_1'
        request = maker(schema)
        newhelper = WMWorkloadHelper(request['WorkflowSpec'])
        # can't save Request object directly, because it makes it hard to retrieve the _rev
        metadata = {}
        metadata.update(request)
        # don't want to JSONify the whole workflow
        del metadata['WorkflowSpec']
        request['RequestWorkflow'] = newhelper.saveCouch(self.reqmgrurl, self.reqmgrname, metadata=metadata)
        self._inject(request)

    def status(self, workflow, userdn):
        """Retrieve the status of the workflow

           :arg str workflow: a valid workflow name
           :return: a workflow status summary document"""

        self.logger.info("Getting status for workflow %s" % workflow)

        row = self.api.query(None, None, "SELECT tm_task_status FROM tasks WHERE tm_taskname = :taskname", taskname = workflow)
        status = row.next()[0] #just one row is picked up by the previous query
        self.logger.debug("Status result for workflow %s: %s" % (workflow, status))

        rows = self.api.query(None, None, "SELECT panda_jobdef_id FROM jobgroups WHERE tm_taskname = :taskname", taskname = workflow)
        for jobdef in row:
            status, pandaIDstatus = PandaBooking(userdn,'cms','','').getPandIDsWithJobID(jobdef[0], True)

        return pandaIDstatus

    def kill(self, workflow):
        """Request to Abort a workflow.

           :arg str workflow: a workflow name"""
        try:
            ChangeState.changeRequestStatus(workflow, 'aborted', wmstatUrl=self.wmstatsurl)
            abortRequest(workflow)
        except RuntimeError, re:
            raise ExecutionError("Problem killing the request", trace=traceback.format_exc(), errobj=re)

    def getType(self, workflow):
        """Retrieves the workflow type from the monitoring

           :arg str workflow: a workflow name
           :return: a string of the job type supported by the workflow."""
        return self.monitordb.conn.document(id=workflow).get('request_type', None)

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
