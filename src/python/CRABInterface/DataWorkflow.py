import time
import threading
import logging
import cherrypy #cherrypy import is needed here because we need the 'start_thread' subscription
import traceback
import json

from CRABInterface.Utils import getDBinstance
# WMCore dependecies here
from WMCore.REST.Error import ExecutionError, InvalidParameter
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON

#CRAB dependencies
from CRABInterface.Utils import CMSSitesCache, conn_handler, retrieveUserCert

def strip_username_from_taskname(workflow):
    """When auto-generating a destination directory and dataset name for
       a given task, we want to remove the username (it appears elsewhere) and
       make sure it doesn't contain ':' (not a valid character in HDFS directory names).
       """
    info = workflow.split("_")
    if len(info) > 3:
        info = info[:2] + info[3:]
        return "_".join(info)
    return workflow.replace(":", "_")

class DataWorkflow(object):
    """Entity that allows to operate on workflow resources.
       No aggregation of workflows provided here."""

    @staticmethod
    def globalinit(dbapi, phedexargs=None, credpath='/tmp', centralcfg=None, config=None):
        DataWorkflow.api = dbapi
        DataWorkflow.phedexargs = phedexargs
        DataWorkflow.phedex = None
        DataWorkflow.credpath = credpath
        DataWorkflow.centralcfg = centralcfg
        DataWorkflow.config = config

    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger("CRABLogger.DataWorkflow")
        self.allCMSNames = CMSSitesCache(cachetime=0, sites={})

        self.splitArgMap = { "LumiBased" : "lumis_per_job",
                        "FileBased" : "files_per_job",
                        "EventBased" : "events_per_job",}

	self.Task = getDBinstance(config, 'TaskDB', 'Task')
	self.JobGroup = getDBinstance(config, 'TaskDB', 'JobGroup')
	self.FileMetaData = getDBinstance(config, 'FileMetaDataDB', 'FileMetaData')

    def updateRequest(self, workflow):
        """Provide the implementing class a chance to rename the workflow
           before it is committed to the DB.
           """
        return workflow

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
               userhn, userdn, savelogsflag, publication, publishname, asyncdest, dbsurl, publishdbsurl, vorole, vogroup, tfileoutfiles, edmoutfiles,\
               runs, lumis, totalunits, adduserfiles, oneEventMode=False, maxjobruntime=None, numcores=None, maxmemory=None, priority=None, userproxy=None):
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
           :arg str dbsurl: dbs url where the input dataset is published;
           :arg str publishdbsurl: dbs url where the output data has to be published;
           :arg str vorole: user vo role
           :arg str vogroup: user vo group
           :arg str tfileoutfiles: list of t-output files
           :arg str edmoutfiles: list of edm output files
           :arg str list runs: list of run numbers
           :arg str list lumis: list of lumi section numbers
           :arg int totalunits: number of MC event to be generated
           :arg str list adduserfiles: list of additional user input files
           :arg str oneEventMode: toggle one event mode
           :arg int maxjobruntime: max job runtime, in minutes
           :arg int numcores: number of CPU cores required by job
           :arg int maxmemory: maximum amount of RAM required, in MB
           :arg int priority: priority of this task
           :returns: a dict which contaians details of the request"""

        #if scheduler == 'condor':
            # TODO: hook this around properly
        #    raise NotImplementedError, "Need to add the proper CRABServer hooks for condor"
        timestamp = time.strftime('%y%m%d_%H%M%S', time.gmtime())
        requestname = self.updateRequest('%s_%s_%s' % (timestamp, userhn, workflow))
        splitArgName = self.splitArgMap[splitalgo]
        dbSerializer = str

        if numcores == None: numcores = 1
        if maxjobruntime == None: maxjobruntime = 1315
        if maxmemory == None: maxmemory = 2000
        if priority == None: priority = 10

        self.api.modify(self.Task.New_sql,
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
                            publish_name    = [(strip_username_from_taskname(requestname) + '-' + publishname) if publishname.find('-')==-1 else publishname],\
                            asyncdest       = [asyncdest],\
                            dbs_url         = [dbsurl],\
                            publish_dbs_url = [publishdbsurl],\
                            publication     = ['T' if publication else 'F'],\
                            outfiles        = [dbSerializer(addoutputfiles)],\
                            tfile_outfiles  = [dbSerializer(tfileoutfiles)],\
                            edm_outfiles    = [dbSerializer(edmoutfiles)],\
                            transformation  = [self.centralcfg.centralconfig["transformation"][jobtype]],\
                            job_type        = [jobtype],\
                            arguments       = [dbSerializer({'oneEventMode' : 'T' if oneEventMode else 'F'})],\
                            resubmitted_jobs= [dbSerializer([])],\
                            save_logs       = ['T' if savelogsflag else 'F'],\
                            user_infiles    = [dbSerializer(adduserfiles)],
                            maxjobruntime   = [maxjobruntime],
                            numcores        = [numcores],
                            maxmemory       = [maxmemory],
                            priority        = [priority],
        )

        return [{'RequestName': requestname}]

    def resubmit(self, workflow, siteblacklist, sitewhitelist, jobids, maxjobruntime, numcores, maxmemory, priority, userdn, userproxy):
        """Request to reprocess what the workflow hasn't finished to reprocess.
           This needs to create a new workflow in the same campaign

           :arg str workflow: a valid workflow name
           :arg str list siteblacklist: black list of sites, with CMS name;
           :arg str list sitewhitelist: white list of sites, with CMS name."""

        retmsg = "ok"
        self.logger.info("About to resubmit workflow: %s. Getting status first." % workflow)
        statusRes = self.status(workflow, userdn, userproxy)[0]

        #if there are failed jobdef submission we fail
        #if statusRes['failedJobdefs']:
        #    raise ExecutionError("You cannot resubmit a task if not all the jobs have been submitted. The feature will be available in the future")

        if statusRes['status'] in ['SUBMITTED','KILLED','FAILED']:
            resubmitList = [jobid for jobstatus,jobid in statusRes['jobList'] if jobstatus in self.failedList]
            if jobids:
                #if the user wants to kill specific jobids make the intersection
                resubmitList = list(set(resubmitList) & set(jobids))
                #check if all the requested jobids can be resubmitted
                if len(resubmitList) != len(jobids):
                    retmsg = "Cannot request resubmission for %s" % str(list(set(jobids) - set(resubmitList)))
                    return [{"result":retmsg}]
            #if not resubmitList:
            #    raise ExecutionError("There are no jobs to resubmit. Only jobs in %s states are resubmitted" % self.failedList)
            self.logger.info("Jobs to resubmit: %s" % resubmitList)
            args = str({"siteBlackList":siteblacklist, "siteWhiteList":sitewhitelist, "resubmitList":resubmitList})
            if maxjobruntime != None:
                args['maxjobruntime'] = maxjobruntime
            if numcores != None:
                args['numcores'] = numcores
            if maxmemory != None:
                args['maxmemory'] = maxmemory
            if priority != None:
                args['priority'] = priority
            self.api.modify(self.Task.SetArgumentsTask_sql, taskname = [workflow],\
                            arguments = [args])
            self.api.modify(self.Task.SetStatusTask_sql, status = ["RESUBMIT"], taskname = [workflow])
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
                self.api.modify(self.Task.SetStatusTask_sql, status = ["COMPLETED"], taskname = [workflow])
                return "COMPLETED"
            #only failed and completed jobs (completed may not be there) => task failed
            if not set(jobsPerStatus) - set(self.successList) - set(self.failedList):
                self.logger.debug("Changing task status to FAILED")
                self.api.modify(self.Task.SetStatusTask_sql, status = ["FAILED"], taskname = [workflow])
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

            self.api.modify(self.Task.SetStatusTask_sql, status = ["KILL"], taskname = [workflow])
            self.api.modify(self.Task.SetArgumentsTask_sql, taskname = [workflow],\
                            arguments = [str({"killList": killList, "killAll": jobids==[]})])
        elif statusRes['status'] == 'NEW':
            self.api.modify(self.Task.SetStatusTask_sql, status = ["KILLED"], taskname = [workflow])
        else:
            raise ExecutionError("You cannot kill a task if it is in the %s state" % statusRes['status'])

        return [{"result":retmsg}]
