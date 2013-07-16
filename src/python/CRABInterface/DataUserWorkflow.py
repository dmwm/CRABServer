import logging
import time
import commands
import json

# WMCore dependecies here
from WMCore.REST.Error import InvalidParameter, ExecutionError, MissingObject

#CRAB dependencies
from CRABInterface.DataWorkflow import DataWorkflow
from CRABInterface.Utils import conn_handler, retrieveUserCert

from WMCore.Database.CMSCouch import CouchServer
import datetime
import traceback
import time

class DataUserWorkflow(object):
    """
    """

    @staticmethod
    def globalinit(workflowManager):
        DataUserWorkflow.workflowManager = workflowManager

    def __init__(self):
        self.logger = logging.getLogger("CRABLogger.DataUserWorkflow")
        mod = __import__('CRABInterface.%s' % self.workflowManager, fromlist=self.workflowManager)
        self.workflow = getattr(mod, self.workflowManager)()

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
        raise NotImplementedError

    @retrieveUserCert
    def report(self, workflow, userdn, userproxy):
        """Retrieves the quality of the workflow in term of what has been processed
           (eg: good lumis). This can call a different function depending on the jobtype.

           :arg str workflow: a workflow name
           :return: what?"""
        return self.workflow.report(workflow, userdn, userproxy)

    @retrieveUserCert
    def logs(self, workflow, howmany, exitcode, jobids, userdn, userproxy=None):
        """Returns the workflow logs PFN. It takes care of the LFN - PFN conversion too.

           :arg str workflow: a workflow name
           :arg int howmany: the limit on the number of PFN to return
           :arg int exitcode: the log has to be of a job ended with this exit_code
           :return: a generator of list of logs pfns"""
        return self.workflow.logs(workflow, howmany, exitcode, jobids, userdn, userproxy)

    @retrieveUserCert
    def output(self, workflow, howmany, jobids, userdn, userproxy=None):
        """Returns the workflow output PFN. It takes care of the LFN - PFN conversion too.

           :arg str list workflow: a workflow name
           :arg int howmany: the limit on the number of PFN to return
           :return: a generator of list of output pfns"""
        return self.workflow.output(workflow, howmany, jobids, userdn, userproxy)

    def submit(self, *args, **kwargs):
        """Perform the workflow injection

           :arg str workflow: workflow name requested by the user;
           :arg str jobtype: job type of the workflow, usually Analysis;
           :arg str jobsw: software requirement;
           :arg str jobarch: software architecture (=SCRAM_ARCH);
           :arg str inputdata: input dataset;
           :arg str list siteblacklist: black list of sites, with CMS name;
           :arg str list sitewhitelist: white list of sites, with CMS name;
           :arg str splitalgo: algorithm to be used for the workflow splitting;
           :arg str algoargs: argument to be used by the splitting algorithm;
           :arg str cachefilename: name of the file inside the cache
           :arg str cacheurl: URL of the cache
           :arg str list addoutputfiles: list of additional output files;
           :arg str userdn: DN of user doing the request;
           :arg str userhn: hyper new name of the user doing the request;
           :arg int savelogsflag: archive the log files? 0 no, everything else yes;
           :arg int publication: flag enabling or disabling data publication;
           :arg str publishname: name to use for data publication;
           :arg str asyncdest: CMS site name for storage destination of the output files;
           :arg int blacklistT1: flag enabling or disabling the black listing of Tier-1 sites;
           :arg str dbsurl: dbs url where the input dataset is published;
           :arg str publishdbsurl: dbs url where the output data has to be published;
           :arg str vorole: user vo role
           :arg str vogroup: user vo group
           :arg str tfileoutfiles: list of t-output files
           :arg str edmoutfiles: list of edm output files
           :arg str list runs: list of run numbers
           :arg str list lumis: list of lumi section numbers
           :arg int totalunits: number of MC event to be generated
           :returns: a dict which contaians details of the request"""

        return self.workflow.submit(*args, **kwargs)

    @retrieveUserCert
    def resubmit(self, workflow, siteblacklist, sitewhitelist, userdn, userproxy=None):
        """Request to Resubmit a workflow.

           :arg str workflow: a workflow name
           :arg int force: force to delete the workflows in any case; 0 no, everything else yes"""
        return self.workflow.resubmit(workflow, siteblacklist, sitewhitelist, userdn, userproxy)

    @retrieveUserCert
    def status(self, workflow, userdn, userproxy=None):
        """Retrieve the status of the workflow

           :arg str workflow: a valid workflow name
           :arg str userdn: the user dn makind the request
           :arg str userproxy: the user proxy retrieved by `retrieveUserCert`
           :return: a generator of workflow states
        """
        return self.workflow.status(workflow, userdn, userproxy)

    @retrieveUserCert
    def kill(self, workflow, force, userdn, userproxy=None):
        """Request to Abort a workflow.

           :arg str workflow: a workflow name
           :arg str force: a flag to know if kill should be brutal
           :arg str userproxy: the user proxy retrieved by `retrieveUserCert`
           :arg int force: force to delete the workflows in any case; 0 no, everything else yes"""
        return self.workflow.kill(workflow, force, userdn, userproxy)


    @retrieveUserCert
    def ASOkill(self, workflow, istance , userdn, userproxy=None):
        self.server = CouchServer(istance)
        try:
            self.db = self.server.connectDatabase('asynctransfer')
        except Exception, ex:
            msg =  "Error while connecting to asynctransfer couchDB  "
            raise ExecutionError(msg)
        self.queryKill = {'reduce':False, 'key':workflow}
        try:
            self.filesKill = self.db.loadView('AsyncTransfer', 'forKill', self.queryKill)['rows']
        except Exception, ex:
            msg =  "Error while connecting to asynctransfer couchDB  "
            raise ExecutionError(msg)
        if len(self.filesKill) == 0:
            raise ExecutionError('No files to kill found')
        for idt in self.filesKill:
            now = str(datetime.datetime.now())
            id = idt['value']
            data = {}
            data['end_time'] = now
            data['state'] = 'killed'
            data['last_update'] = time.time()
            data['retry'] = now
            updateUri = "/" + self.db.name + "/_design/AsyncTransfer/_update/updateJobs/" + id
            updateUri += "?" + urllib.urlencode(data)
            try:
                self.db.makeRequest(uri = updateUri, type = "PUT", decode = False)
            except Exception, ex:
                msg =  "Error updating document in couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                raise ExecutionError(msg)

    @retrieveUserCert
    def ASOresubmit(self, workflow, istance, userdn, userproxy=None):
        self.server = CouchServer(istance)
        try:
            self.db = self.server.connectDatabase('asynctransfer')
        except Exception, ex:
            msg =  "Error while connecting to asynctransfer couchDB "
            raise ExecutionError(msg)
        self.queryResub = {'reduce':False, 'key':workflow}
        self.filesResub = self.db.loadView('AsyncTransfer', 'forResub', self.queryResub)['rows']
        if len(self.filesResub) == 0:
            raise ExecutionError('No files to resubmit found')
        for idt in self.filesResub:
            id = idt['value']
            data = {}
            data['state'] = 'new'
            data['last_update'] = time.time()
            updateUri = "/" + self.db.name + "/_design/AsyncTransfer/_update/updateJobs/" + id
            updateUri += "?" + urllib.urlencode(data)
            try:
                self.db.makeRequest(uri = updateUri, type = "PUT", decode = False)
            except Exception, ex:
                msg =  "Error updating document in couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                raise ExecutionError(msg)

