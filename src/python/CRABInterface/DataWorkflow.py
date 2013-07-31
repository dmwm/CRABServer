import time
import threading
import logging
import cherrypy #cherrypy import is needed here because we need the 'start_thread' subscription
import traceback
import json
from hashlib import sha1

# WMCore dependecies here
from WMCore.REST.Error import ExecutionError, InvalidParameter
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON

#CRAB dependencies
from CRABInterface.Utils import CMSSitesCache, conn_handler, retrieveUserCert

#SQL queries
from Databases.TaskDB.Oracle.Task.New import New
from Databases.TaskDB.Oracle.Task.SetStatusTask import SetStatusTask
from Databases.TaskDB.Oracle.Task.SetArgumentsTask import SetArgumentsTask

class DataWorkflow(object):
    """Entity that allows to operate on workflow resources.
       No aggregation of workflows provided here."""

    @staticmethod
    def globalinit(dbapi, phedexargs=None, dbsurl=None, credpath='/tmp', centralcfg=None):
        DataWorkflow.api = dbapi
        DataWorkflow.phedexargs = phedexargs
        DataWorkflow.phedex = None
        DataWorkflow.dbsurl = dbsurl
        DataWorkflow.credpath = credpath
        DataWorkflow.centralcfg = centralcfg

    def __init__(self):
        self.logger = logging.getLogger("CRABLogger.DataWorkflow")
        self.allCMSNames = CMSSitesCache(cachetime=0, sites={})

        self.splitArgMap = { "LumiBased" : "lumis_per_job",
                        "FileBased" : "files_per_job",
                        "EventBased" : "events_per_job",}


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

    def logs(self, workflow, howmany, exitcode, jobids):
        """Returns the workflow logs PFN. It takes care of the LFN - PFN conversion too.

           :arg str workflow: a workflow name
           :arg int howmany: the limit on the number of PFN to return
           :arg int exitcode: the log has to be of a job ended with this exit_code
           :return: (a generator of?) a list of logs pfns"""
        raise NotImplementedError

    def output(self, workflow, howmany, jobids):
        """
        Retrieves the output PFN aggregating output in final and temporary locations.

        :arg str workflow: the unique workflow name
           :arg int howmany: the limit on the number of PFN to return
           :return: a generator of list of outputs"""
        raise NotImplementedError

    def schema(self, workflow):
        """Returns the workflow schema parameters.

           :arg str workflow: a workflow name
           :return: a json corresponding to the workflow schema"""
        # it probably needs to connect to the database
        # TODO: verify + code the above point
        # probably we need to explicitely select the schema parameters to return
        raise NotImplementedError

    @conn_handler(services=['centralconfig'])
    @retrieveUserCert
    def submit(self, workflow, jobtype, jobsw, jobarch, inputdata, siteblacklist, sitewhitelist, splitalgo, algoargs, cachefilename, cacheurl, addoutputfiles,\
               userhn, userdn, savelogsflag, publication, publishname, asyncdest, blacklistT1, dbsurl, publishdbsurl, vorole, vogroup, tfileoutfiles, edmoutfiles,\
               runs, lumis, totalunits, userproxy=None):
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
           :arg int savelogsflag: archive the log files? 0 no, everything else yes;
           :arg str userdn: DN of user doing the request;
           :arg str userhn: hyper new name of the user doing the request;
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

        timestamp = time.strftime('%y%m%d_%H%M%S', time.gmtime())
        requestname = '%s_%s_%s' % (timestamp, userhn, workflow)
        splitArgName = self.splitArgMap[splitalgo]
        dbSerializer = str

        self.api.modify(New.sql,
                            task_name       = [requestname],\
                            jobset_id       = [None],
                            task_status     = ['NEW'],\
                            task_failure    = [''],\
                            job_sw          = [jobsw],\
                            job_arch        = [jobarch],\
                            input_dataset   = [inputdata],\
                            site_whitelist  = [dbSerializer(sitewhitelist)],\
                            site_blacklist  = [dbSerializer(siteblacklist)],\
                            split_algo      = [splitalgo],\
                            split_args      = [dbSerializer({'halt_job_on_file_boundaries': False, 'splitOnRun': False,\
                                                splitArgName : algoargs, 'runs': runs, 'lumis': lumis})],\
                            total_units     = [totalunits],\
                            user_sandbox    = [cachefilename],\
                            cache_url       = [cacheurl],\
                            username        = [userhn],\
                            user_dn         = [userdn],\
                            user_vo         = ['cms'],\
                            user_role       = [vorole],\
                            user_group      = [vogroup],\
                            publish_name    = [(timestamp + '-' + publishname) if publishname.find('-')==-1 else publishname],\
                            asyncdest       = [asyncdest],\
                            dbs_url         = [dbsurl or self.dbsurl],\
                            publish_dbs_url = [publishdbsurl],\
                            publication     = ['T' if publication else 'F'],\
                            outfiles        = [dbSerializer(addoutputfiles)],\
                            tfile_outfiles  = [dbSerializer(tfileoutfiles)],\
                            edm_outfiles    = [dbSerializer(edmoutfiles)],\
                            transformation  = [self.centralcfg.centralconfig["transformation"][jobtype]],\
                            job_type        = [jobtype],\
                            arguments       = [dbSerializer({})],\
                            resubmitted_jobs= [dbSerializer([])],\
                            save_logs       = ['T' if savelogsflag else 'F']\

        )

        return [{'RequestName': requestname}]

    def resubmit(self, workflow, siteblacklist, sitewhitelist, jobids, userdn, userproxy):
        """Request to reprocess what the workflow hasn't finished to reprocess.
           This needs to create a new workflow in the same campaign

           :arg str workflow: a valid workflow name
           :arg str list siteblacklist: black list of sites, with CMS name;
           :arg str list sitewhitelist: white list of sites, with CMS name."""

        retmsg = "ok"
        self.logger.info("About to resubmit workflow: %s. Getting status first." % workflow)
        statusRes = self.status(workflow, userdn, userproxy)[0]

        #if there are failed jobdef submission we fail
        if statusRes['failedJobdefs']:
            raise ExecutionError("You cannot resubmit a task if not all the jobs have been submitted. The feature will be available in the future")

        if statusRes['status'] in ['SUBMITTED','KILLED','FAILED']:
            resubmitList = [jobid for jobstatus,jobid in statusRes['jobList'] if jobstatus in self.failedList]
            if jobids:
                #if the user wants to kill specific jobids make the intersection
                resubmitList = list(set(resubmitList) & set(jobids))
                #check if all the requested jobids can be resubmitted
                if len(resubmitList) != len(jobids):
                    retmsg = "Cannot request resubmission for %s" % (set(jobids) - set(resubmitList))
            if not resubmitList:
                raise ExecutionError("There are no jobs to resubmit. Only jobs in %s states are resubmitted" % self.failedList)
            self.logger.info("Jobs to resubmit: %s" % resubmitList)
            self.api.modify(SetStatusTask.sql, status = ["RESUBMIT"], taskname = [workflow])
            self.api.modify(SetArgumentsTask.sql, taskname = [workflow],\
                            arguments = [str({"siteBlackList":siteblacklist, "siteWhiteList":sitewhitelist, "resubmitList":resubmitList})])
        else:
            raise ExecutionError("You cannot resubmit a task if it is in the %s state" % statusRes['status'])

        return [{"result":retmsg}]


    def _updateTaskStatus(self, workflow, status, jobsPerStatus):
        """
        Update the status of the task when it is finished.
        More details: if the status of the task is submitted => all the jobs are finished then taskStatus=COMPLETED
                                                             => all the jobs are finished or failed then taskStatus=FAILED
        """
        if status=='SUBMITTED':
            #only completed jobs
            if not set(jobsPerStatus) - set(self.successList):
                self.logger.debug("Changing task status to COMPLETED")
                self.api.modify(SetStatusTask.sql, status = ["COMPLETED"], taskname = [workflow])
                return "COMPLETED"
            #only failed and completed jobs (completed may not be there) => task failed
            if not set(jobsPerStatus) - set(self.successList) - set(self.failedList):
                self.logger.debug("Changing task status to FAILED")
                self.api.modify(SetStatusTask.sql, status = ["FAILED"], taskname = [workflow])
                return "FAILED"
        return status


    def status(self, workflow, userdn, userproxy=None):
        """Retrieve the status of the workflow.

           :arg str workflow: a valid workflow name
           :return: a workflow status summary document"""
        raise NotImplementedError

    def kill(self, workflow, force, jobids, userdn, userproxy=None):
        """Request to Abort a workflow.

           :arg str workflow: a workflow name"""

        retmsg = "ok"
        self.logger.info("About to kill workflow: %s. Getting status first." % workflow)
        statusRes = self.status(workflow, userdn, userproxy)[0]

        if statusRes['status'] == 'SUBMITTED':
            killList = [jobid for jobstatus,jobid in statusRes['jobList'] if jobstatus not in self.successList]
            if jobids:
                #if the user wants to kill specific jobids make the intersection
                killList = list(set(killList) & set(jobids))
                #check if all the requested jobids can be resubmitted
                if len(killList) != len(jobids):
                    retmsg = "Cannot request kill for %s" % (set(jobids) - set(killList))
            if not killList:
                raise ExecutionError("There are no jobs to kill. Only jobs not in %s states can be killed" % self.successList)
            self.logger.info("Jobs to kill: %s" % killList)

            self.api.modify(SetStatusTask.sql, status = ["KILL"], taskname = [workflow])
            self.api.modify(SetArgumentsTask.sql, taskname = [workflow],\
                            arguments = [str({"killList": killList, "killAll": jobids==[]})])
        elif statusRes['status'] == 'NEW':
            self.api.modify(SetStatusTask.sql, status = ["KILLED"], taskname = [workflow])
        else:
            raise ExecutionError("You cannot kill a task if it is in the %s state" % statusRes['status'])

        return [{"result":retmsg}]
