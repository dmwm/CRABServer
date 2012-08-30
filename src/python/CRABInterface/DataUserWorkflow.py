import logging
import time

# WMCore dependecies here
from WMCore.REST.Error import InvalidParameter, ExecutionError, MissingObject
from WMCore.Database.CMSCouch import CouchServer, CouchError, CouchNotFoundError

#CRAB dependencies
from CRABInterface.DataWorkflow import DataWorkflow
from CRABInterface.Utils import CouchDBConn, conn_handler


class DataUserWorkflow(object):
    """Entity that allows to operate on workflow resources from the user point of view.
       This class exploits the funcionalities of the DataWorkflow class to retrieve
       single workflow information, by aggregating the data of various chained workflows.
       Chained workflows are related because of resubmission:
         - each time there is a resubmission a new workflow is created with the previous
           workflow as parent;
         - this is completely hidden to the user which will always see the single
           original workflow;
         - this class hides the resubmitted workflows, providing an aggregated view
           of the 'chianed workflows'.

       Note. Current implementation uses the concept of campaign to aggregate workflows.
             This is valid till no campaign use cases are supported. The correct way is to
             use a field in the monitoring with the name of parent workflow (original
             workflows will have the campaign name as parent)."""

    @staticmethod
    def globalinit(monurl, monname, asomonurl, asomonname):
        DataUserWorkflow.monitordb = CouchDBConn(db=CouchServer(monurl), name=monname, conn=None)
        DataUserWorkflow.asodb = CouchDBConn(db=CouchServer(asomonurl), name=asomonname, conn=None)

        DataUserWorkflow.wmstatsurl = monurl + '/' + monname
        DataUserWorkflow.wmstats = None

    def __init__(self):
        self.logger = logging.getLogger("CRABLogger.DataUserWorkflow")
        self.workflow = DataWorkflow()

    @conn_handler(services=['monitor'])
    def _getWorkflowChain(self, wf):
        """Starting from a workflow name it retrieves the chain
           of resubmitted workflows.
           Gets all the 'reqmgr_request' documents children of
           the workflow, which corresponds to the resubmitted
           workflows (one for each wf).

           TODO This will need to be changed as soon as campaign use cases are supported.
                Read class doc to get a better idea.

           :arg str wf: root workflow name, which is the first submitted workflow
           :return: the ordered list of workflows, ordered by re-submission time."""
        options = {"startkey": [wf,0], "endkey": [wf,{}]}
        try:
            workflowdocs = self.monitordb.conn.loadView("WMStats", "requestByCampaignAndDate", options)
        except CouchNotFoundError, ce:
            raise ExecutionError("Impossible to load requestByCampaignAndDate view from WMStats")
        if not workflowdocs['rows']:
            raise MissingObject("Cannot find requested workflow")

        return [doc['value']['id'] for doc in workflowdocs['rows']]

    def _aggregateChainStatus(self, wfdocs):
        """A resubmission keeps succeded jobs, and, after a kill,
           sends a new workflow on the not/analyzed part of the
           dataset (parts where jobs are running/scheduled/failed/etc).
           In the aggregation we iterates on 'old' workflows and sum
           the number of succeded jobs. The last resubmission is kept
           as it is.

           :arg dict list wfdocs: a list with the workflow document summary
           :return: two dicts, one with the high-level aggregation per
                    state and another one with the details for each
                    state."""
        jobsperstate = {}
        detailsperstate = {}
        if not wfdocs:
            return jobsperstate, detailsperstate

        #jobs in success state are taken from all the workflows
        jobsperstate['success'] = sum( [ doc['success'] for doc in wfdocs if doc.get('success', None) ] )

        lastdoc = wfdocs[-1]
        for state in lastdoc:
            if state != 'inWMBS':
                if type(lastdoc[state])==int:
                    jobsperstate[state] = lastdoc[state]
                else:
                    jobsperstate[state] = sum( lastdoc[state].values() )
                    detailsperstate[state] = lastdoc[state]
                    #count back retry and first from the total
                    if state == 'submitted' and 'submitted' in lastdoc:
                        jobsperstate['submitted'] -= lastdoc['submitted'].get('first', 0)
                        jobsperstate['submitted'] -= lastdoc['submitted'].get('retry', 0)

        return jobsperstate, detailsperstate

    @conn_handler(services=['monitor'])
    def getLatests(self, user, limit, timestamp):
        """Retrives the latest workflows for the user

           :arg str user: a valid user hn login name
           :arg int limit: the maximum number of workflows to return
                          (this should probably have a default!)
           :arg int timestamp: limit on the workflow age. This is an integer ad returnet by time.gmtime
           :return: a list of workflows"""

        starttime = list(time.gmtime(timestamp))[0:6]
        endtime   = list(time.gmtime())[0:6]

        #not using limit because of resubmissions :(
        options = {"startkey": [starttime, user], "endkey": [endtime, user]}
        try:
            docs = self.monitordb.conn.loadView("WMStats", "requestByDate", options)
        except CouchNotFoundError, ce:
            raise ExecutionError("Impossible to load requestByDate view from WMStats")

        #take only the last workflow in the campaign (last resubmission)
        result = {}
        for row in docs['rows']:
            result[row['value']['campaign']] = row

        #return the list ordered by date (we lost the order in the previous for)
        return sorted( result.values(), key=lambda x: x['key'][0])


    @conn_handler(services=['monitor'])
    def errors(self, workflow, shortformat):
        """Retrieves the sets of errors for a specific workflow

           :arg str workflow: a workflow name
           :arg int shortformat: a flag indicating if the user is asking for detailed
                                 information about sites and list of errors
           :return: a list of errors grouped by exit code, error reason, site"""
        wfchain = self._getWorkflowChain(workflow)

        #TODO: how do we merge these?
        for wf in wfchain:
            yield self.workflow.errors(wf, shortformat)

    @conn_handler(services=['monitor'])
    def report(self, workflow):
        """Retrieves the quality of the workflow in term of what has been processed
           (eg: good lumis). This can call a different function depending on the jobtype.

           :arg str workflow: a workflow name
           :return: what?"""
        wfchain = self._getWorkflowChain(workflow)
        for wf in wfchain:
            yield self.workflow.report(wf)

    @conn_handler(services=['monitor'])
    def logs(self, workflow, howmany, exitcode):
        """Returns the workflow logs PFN. It takes care of the LFN - PFN conversion too.

           :arg str workflow: a workflow name
           :arg int howmany: the limit on the number of PFN to return
           :arg int exitcode: the log has to be of a job ended with this exit_code
           :return: a generator of list of logs pfns"""
        wfchain = self._getWorkflowChain(workflow)
        #TODO: this is now returning duplicated logs for previously submitted workflows who have failed
        #      need to fix this in order to avoid duplicates as soon as resubmission are supported
        for wf in wfchain:
            for el in self.workflow.logs(wf, howmany, exitcode):
                yield el

    @conn_handler(services=['monitor'])
    def fwjr(self, workflow, howmany, exitcode):
        """Returns the the error messages of the fwjr reported by the monitoring.

           :arg str workflow: a workflow name
           :arg int howmany: the limit on the number of fwjr to return
           :arg int exitcode: the fwjr has to be of a job ended with this exit_code
           :return: a generator of list of fwjr"""
        wfchain = self._getWorkflowChain(workflow)
        #return the errors of just the last workflow. Failed jobs of older wf have been resubmitted
        if wfchain:
            return self.workflow.fwjr(wfchain[-1], howmany, exitcode)

    @conn_handler(services=['monitor', 'asomonitor'])
    def output(self, workflow, howmany):
        """Returns the workflow output PFN. It takes care of the LFN - PFN conversion too.

           :arg str list workflow: a workflow name
           :arg int howmany: the limit on the number of PFN to return
           :return: a generator of list of output pfns"""
        wfchain = self._getWorkflowChain(workflow)
        howmany = howmany if howmany else 1
        max = howmany if howmany > 0 else None
        result = []
        for wf in wfchain:
            result += self.workflow.outputLocation(wf, max)
            # check if we have got enough and how many missing
            if max:
                if len(result) >= howmany:
                    break
                else:
                    max = howmany - len(result)

        if max and len(result) < howmany:
            jobids = [singlefile['value']['jobid'] for singlefile in result]
            tempresult = []
            for wf in wfchain:
                tempresult += self.workflow.outputTempLocation(wf, howmany-len(result), jobids)
                if len(tempresult) + len(result) >= howmany:
                    result += tempresult[: howmany-len(result)]
                    break
                else:
                    result += tempresult

        return self.workflow.getPhyisicalLocation(result)

    def schema(self, workflow):
        """Returns the workflow schema parameters.

           :arg str workflow: a workflow name
           :return: a json corresponding to the workflow schema"""
        # it probably needs to connect to the reqmgr couch database
        # TODO: verify + code the above point
        # probably we need to explicitely select the schema parameters to return
        wfchain = self._getWorkflowChain(workflow)

        raise NotImplementedError

    def configcache(self, workflow):
        """Returns the config cache associated to the workflow.

           :arg str workflow: a workflow name
           :return: the config cache couch json object"""
        # it probably needs to connect to the reqmgr and config cache couch databases
        # TODO: verify + code the above point
        wfchain = self._getWorkflowChain(workflow)
        raise NotImplementedError

    def submit(self, workflow, jobtype, jobsw, jobarch, inputdata, siteblacklist, sitewhitelist, blockwhitelist,
               blockblacklist, splitalgo, algoargs, configdoc, userisburl, adduserfiles, addoutputfiles, savelogsflag,
               userdn, userhn, publishname, asyncdest, campaign, blacklistT1, dbsurl, publishdbsurl, acdcdoc, globaltag):
        """Perform the workflow injection into the reqmgr + couch

           :arg str workflow: workflow name requested by the user;
           :arg str jobtype: job type of the workflow, usually Analysis;
           :arg str jobsw: software requirement;
           :arg str jobarch: software architecture (=SCRAM_ARCH);
           :arg str list inputdata: input datasets;
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
           :arg str publishname: name to use for data publication;
           :arg str asyncdest: CMS site name for storage destination of the output files;
           :arg str campaign: needed just in case the workflow has to be appended to an existing campaign;
           :arg int blacklistT1: flag enabling or disabling the black listing of Tier-1 sites;
           :arg str dbsurl: dbs url where the input dataset is published;
           :arg str publishdbsurl: dbs url where the output data has to be published;
           :arg str acdcdoc: input acdc document which contains the input information for data selction (eg: lumi mask)
           :arg str globaltag: the globaltag to use when running the job;
           :returns: a dict which contaians details of the request"""

        return self.workflow.submit(workflow, jobtype, jobsw, jobarch, inputdata, siteblacklist, sitewhitelist,
                                        blockwhitelist, blockblacklist, splitalgo, algoargs, configdoc, userisburl,
                                        adduserfiles, addoutputfiles, savelogsflag, userdn, userhn, publishname,
                                        asyncdest, campaign, blacklistT1, dbsurl, publishdbsurl, acdcdoc, globaltag)

    def resubmit(self, workflow, siteblacklist, sitewhitelist):
        """Request to reprocess what the latest workflow in the chain hasn't finished to reprocess.
           This needs to create a new workflow in the same campaign, using previously missed input.
           :arg str workflow: a valid workflow name
           :arg str list siteblacklist: black list of sites, with CMS name;
           :arg str list sitewhitelist: white list of sites, with CMS name."""
        latestwf = self._getWorkflowChain(workflow)[-1]
        wfdoc = self.workflow.getWorkflow(latestwf)
        if wfdoc['request_status'][-1]['status'] not in ["aborted", "completed"]:
            raise InvalidParameter("Impossible to resubmit a not completed workflow.")
        for status in wfdoc['status']:
            if status not in ["failure", "success"]:
                raise InvalidParameter("Impossible to resubmit a workflow with pending jobs.")
        self.workflow.resubmit(workflow=wfdoc["workflow"], siteblacklist=siteblacklist, sitewhitelist=sitewhitelist)

    def status(self, workflow):
        """Retrieve the status of the workflow

           :arg str workflow: a valid workflow name
           :return: a generator of workflow states"""

        wfchain = self._getWorkflowChain(workflow)
        wfdocs = [self.workflow.getWorkflow(wf) for wf in wfchain]

        #checking that we have some workflow documents
        if not wfdocs:
            raise MissingObject("Cannot find workflow")
        self.logger.debug("Workflow documents retrieved: %s" % wfdocs)

        # TODO: review this, in order to share the code among this and DataWorkflow.status
        jobperstate, detailsperstate = self._aggregateChainStatus([doc['status'] for doc in wfdocs if doc.get('status', None)])

        detailspersite = {}
        #iterate on all the workflows and from them get a list with all the sites
        # TODO: move to DataWorkflow.status
        siteList = reduce(lambda x,y: x+y, [doc['sites'].keys() for doc in wfdocs if doc.get('sites', None)], [])
        #for each site get the aggregation
        for site in siteList:
            detailspersite[site] = self._aggregateChainStatus([doc['sites'][site] for doc in wfdocs if doc.get('sites', None) and doc['sites'].get(site, None)])

        #get the status of the workflow: sort by update_time the request_stauts list of the most recent workflow
        # TODO: move to DataWorkflow.status
        workflowstatus = sorted([x for x in wfdocs[-1]["request_status"]], key=lambda y: y['update_time'] )[-1]['status']

        #successful jobs can be in several states. Taking information from async
        transfers = self.workflow.getWorkflowTransfers(wfdocs[-1]['_id'])
        transfers = {} if 'state' not in transfers else transfers['state']
        if transfers:
            detailsperstate['success'] = transfers

        return [wfdocs[-1]["workflow"], workflowstatus, jobperstate, detailsperstate, detailspersite]

    def kill(self, workflow, force):
        """Request to Abort a workflow.

           :arg str workflow: a workflow name
           :arg int force: force to delete the workflows in any case; 0 no, everything else yes"""
        skipkill = ["aborted", "failed", "completed"]
        latestwf = self._getWorkflowChain(workflow)[-1]
        wfdoc = self.workflow.getWorkflow(latestwf)
        if not force:
            if wfdoc['request_status'][-1]['status'] in skipkill:
                raise InvalidParameter("Workflow cannot be killed because already finished.")
        self.workflow.kill(workflow)

    def getType(self, workflow):
        """Retrieves the workflow type from the monitoring

           :arg str workflow: a workflow name
           :return: a string of the job type supported by the workflow."""
        ## will we have workflows with different job types in the same chain?
        ## current answer is NO
        return self.workflow.getType(self._getWorkflowChain(workflow)[-1])
