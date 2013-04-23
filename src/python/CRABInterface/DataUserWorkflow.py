import logging
import time
import commands
import json

# WMCore dependecies here
from WMCore.REST.Error import InvalidParameter, ExecutionError, MissingObject
from WMCore.Database.CMSCouch import CouchServer, CouchError, CouchNotFoundError
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

#CRAB dependencies
from CRABInterface.DataWorkflow import DataWorkflow
from CRABInterface.Utils import conn_handler
from CRABInterface.Utils import retriveUserCert

#Common solution dependencies
from PandaBooking import PandaBooking
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

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

    def __init__(self):
        self.logger = logging.getLogger("CRABLogger.DataUserWorkflow")
        self.workflow = DataWorkflow()

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
        raise NotImplementedError

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

    def report(self, workflow):
        """Retrieves the quality of the workflow in term of what has been processed
           (eg: good lumis). This can call a different function depending on the jobtype.

           :arg str workflow: a workflow name
           :return: what?"""
        wfchain = self._getWorkflowChain(workflow)
        for wf in wfchain:
            yield self.workflow.report(wf)

    def logs(self, workflow, howmany, exitcode, pandaids):
        """Returns the workflow logs PFN. It takes care of the LFN - PFN conversion too.

           :arg str workflow: a workflow name
           :arg int howmany: the limit on the number of PFN to return
           :arg int exitcode: the log has to be of a job ended with this exit_code
           :return: a generator of list of logs pfns"""
        wfchain = self._getWorkflowChain(workflow)
        #TODO: how do we merge these?
        for wf in wfchain:
            yield self.workflow.logs(wf, shortformat)

    def output(self, workflow, howmany, pandaids):
        """Returns the workflow output PFN. It takes care of the LFN - PFN conversion too.

           :arg str list workflow: a workflow name
           :arg int howmany: the limit on the number of PFN to return
           :return: a generator of list of output pfns"""

        wfchain = self._getWorkflowChain(workflow)
        for wf in wfchain:
            yield self.workflow.output(wf)

    def submit(self, *args, **kwargs):
        """Perform the workflow injection

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
           :arg str list runs: list of run numbers
           :arg str list lumis: list of lumi section numbers
           :returns: a dict which contaians details of the request"""

        return self.workflow.submit(*args, **kwargs)

    def resubmit(self, workflow, siteblacklist, sitewhitelist, userdn):
        """Request to Resubmit a workflow.

           :arg str workflow: a workflow name
           :arg int force: force to delete the workflows in any case; 0 no, everything else yes"""
        return self.workflow.resubmit(workflow, siteblacklist, sitewhitelist, userdn)

    @retriveUserCert(clean=False)
    def status(self, workflow, userdn):
        """Retrieve the status of the workflow

           :arg str workflow: a valid workflow name
           :return: a generator of workflow states
        """
        return self.workflow.status(workflow, userdn)

    @retriveUserCert(clean=False)
    def kill(self, workflow, force, userdn):
        """Request to Abort a workflow.

           :arg str workflow: a workflow name
           :arg int force: force to delete the workflows in any case; 0 no, everything else yes"""
        return self.workflow.kill(workflow, force, userdn)
