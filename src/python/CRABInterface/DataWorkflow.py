import time
import random
import logging
import cherrypy
from datetime import datetime
from ast import literal_eval

## WMCore dependecies
from WMCore.REST.Error import ExecutionError

## CRAB dependencies
from CRABInterface.Utils import CMSSitesCache, conn_handler, getDBinstance


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
                        "EventBased" : "events_per_job",
                        "EventAwareLumiBased": "events_per_job"}

	self.Task = getDBinstance(config, 'TaskDB', 'Task')
	self.JobGroup = getDBinstance(config, 'TaskDB', 'JobGroup')
	self.FileMetaData = getDBinstance(config, 'FileMetaDataDB', 'FileMetaData')

    def updateRequest(self, workflow):
        """Provide the implementing class a chance to rename the workflow
           before it is committed to the DB.
           """
        return workflow

    def getLatests(self, username, timestamp):
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
        return self.api.query(None, None, self.Task.GetTasksFromUser_sql, username=username, timestamp=timestamp)

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
    def submit(self, workflow, activity, jobtype, jobsw, jobarch, inputdata, use_parent, generator, events_per_lumi, siteblacklist, sitewhitelist, splitalgo, algoargs, cachefilename, cacheurl, addoutputfiles,
               userhn, userdn, savelogsflag, publication, publishname, asyncdest, dbsurl, publishdbsurl, vorole, vogroup, tfileoutfiles, edmoutfiles,
               runs, lumis, totalunits, adduserfiles, oneEventMode=False, maxjobruntime=None, numcores=None, maxmemory=None, priority=None, lfn=None,
               ignorelocality=None, saveoutput=None, faillimit=10, userfiles=None, userproxy=None, asourl=None, scriptexe=None, scriptargs=None, scheddname=None,
               extrajdl=None, collector=None, dryrun=False):
        """Perform the workflow injection

           :arg str workflow: workflow name requested by the user;
           :arg str activity: workflow activity type, usually analysis;
           :arg str jobtype: job type of the workflow, usually Analysis;
           :arg str jobsw: software requirement;
           :arg str jobarch: software architecture (=SCRAM_ARCH);
           :arg str inputdata: input dataset;
           :arg int use_parent: add the parent dataset as secondary input;
           :arg str generator: event generator for MC production;
           :arg int events_per_lumi: number of events per lumi to generate
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
           :arg str lfn: lfn used to store output files.
           :arg str userfiles: The files to process instead of a DBS-based dataset.
           :arg str asourl: Specify which ASO to use for transfers and publishing.
           :arg str scheddname: Schedd Name used for debugging.
           :arg str collector: Collector Name used for debugging.
           :arg int dryrun: enable dry run mode (initialize but do not submit task).
           :returns: a dict which contaians details of the request"""

        timestamp = time.strftime('%y%m%d_%H%M%S', time.gmtime())
        requestname = ""
        schedd_name = ""
        backend_urls = self.centralcfg.centralconfig.get("backend-urls", {})
        if collector:
            backend_urls['htcondorPool'] = collector
        else:
            collector = backend_urls['htcondorPool']

        try:
            requestname = '%s:%s_%s' % (timestamp, userhn, workflow)
            schedd_name = self.chooseScheduler(scheddname, backend_urls).split(":")[0]
        except IOError as err:
            self.logger.debug("Failed to communicate with components %s. Request name %s: " % (str(err), str(requestname)))
            raise ExecutionError("Failed to communicate with crabserver components. If problem persist, please report it.")
        splitArgName = self.splitArgMap[splitalgo]
        username = cherrypy.request.user['login']
        dbSerializer = str

        ## If these parameters were not set in the submission request, give them
        ## predefined default values.
        if maxjobruntime is None:
            maxjobruntime = 1315
        if maxmemory is None:
            maxmemory = 2000
        if numcores is None:
            numcores = 1
        if priority is None:
            priority = 10

        if not asourl:
            asourl = self.centralcfg.centralconfig.get("backend-urls", {}).get("ASOURL", "")
            if type(asourl)==list:
                asourl = random.choice(asourl)

        arguments = {}

        ## Insert this new task into the Tasks DB.
        self.api.modify(self.Task.New_sql,
                            task_name       = [requestname],
                            task_activity   = [activity],
                            jobset_id       = [None],
                            task_status     = ['NEW'],
                            task_failure    = [''],
                            job_sw          = [jobsw],
                            job_arch        = [jobarch],
                            input_dataset   = [inputdata],
                            use_parent      = [use_parent],
                            generator       = [generator],
                            events_per_lumi = [events_per_lumi],
                            site_whitelist  = [dbSerializer(sitewhitelist)],
                            site_blacklist  = [dbSerializer(siteblacklist)],
                            split_algo      = [splitalgo],
                            split_args      = [dbSerializer({'halt_job_on_file_boundaries': False, 'splitOnRun': False,
                                                splitArgName : algoargs, 'runs': runs, 'lumis': lumis})],
                            total_units     = [totalunits],
                            user_sandbox    = [cachefilename],
                            cache_url       = [cacheurl],
                            username        = [userhn],
                            user_dn         = [userdn],
                            user_vo         = ['cms'],
                            user_role       = [vorole],
                            user_group      = [vogroup],
                            ## The client defines publishname = <Data.publishDataName>-<isbchecksum>
                            ## if the user defines Data.publishDataName, and publishname = <isbchecksum> otherwise.
                            ## (The PostJob replaces then the isbchecksum by the psethash.)
                            publish_name    = [(workflow.replace(":", "_") + '-' + publishname) if publishname.find('-')==-1 else publishname],
                            asyncdest       = [asyncdest],
                            dbs_url         = [dbsurl],
                            publish_dbs_url = [publishdbsurl],
                            publication     = ['T' if publication else 'F'],
                            outfiles        = [dbSerializer(addoutputfiles)],
                            tfile_outfiles  = [dbSerializer(tfileoutfiles)],
                            edm_outfiles    = [dbSerializer(edmoutfiles)],
                            job_type        = [jobtype],
                            arguments       = [dbSerializer(arguments)],
                            resubmitted_jobs= [dbSerializer([])],
                            save_logs       = ['T' if savelogsflag else 'F'],
                            user_infiles    = [dbSerializer(adduserfiles)],
                            maxjobruntime   = [maxjobruntime],
                            numcores        = [numcores],
                            maxmemory       = [maxmemory],
                            priority        = [priority],
                            scriptexe       = [scriptexe],
                            scriptargs      = [dbSerializer(scriptargs)],
                            extrajdl        = [dbSerializer(extrajdl)],
                            asourl          = [asourl],
                            collector       = [collector],
                            schedd_name     = [schedd_name],
                            dry_run         = ['T' if dryrun else 'F'],
                            user_files       = [dbSerializer(userfiles)],
                            transfer_outputs = ['T' if saveoutput else 'F'],
                            output_lfn       = [lfn],
                            ignore_locality  = ['T' if ignorelocality else 'F'],
                            fail_limit       = [faillimit],
                            one_event_mode   = ['T' if oneEventMode else 'F']
        )

        return [{'RequestName': requestname}]

    def resubmit(self, workflow, siteblacklist, sitewhitelist, jobids, maxjobruntime, numcores, maxmemory, priority, force, userdn, userproxy):
        """Request to reprocess what the workflow hasn't finished to reprocess.
           This needs to create a new workflow in the same campaign

           :arg str workflow: a valid workflow name
           :arg str list siteblacklist: black list of sites, with CMS name;
           :arg str list sitewhitelist: white list of sites, with CMS name."""
        retmsg = "ok"
        self.logger.info("About to resubmit workflow: %s. Getting status first." % (workflow))
        ## Get the status of the task/jobs.
        statusRes = self.status(workflow, userdn, userproxy)[0]
        ## We allow resubmission of jobs only if the task status is one of these:
        allowedTaskStates = ['SUBMITTED', 'KILLED', 'FAILED', 'KILLFAILED']
        ## If the user wants to resubmit a specific set of jobs, then we also accept the
        ## task to be in COMPLETED state. This is because we want to allow resubmission
        ## of successfully finished jobs if the user explicitly gave the job id.
        if jobids and force:
            allowedTaskStates += ['COMPLETED']
        ## If the task status is not an allowed one, fail the resubmission.
        if statusRes['status'] not in allowedTaskStates:
            if statusRes['status'] in ['COMPLETED']:
                msg = "Task status is COMPLETED. To resubmit jobs from a task in status COMPLETED, use the --jobids and --force options."
            else:
                msg = "You cannot resubmit a task if it is in the %s state." % (statusRes['status'])
            raise ExecutionError(msg)
        ## This is the list of job ids that we allow to be resubmitted.
        ## Note: This list will be empty if statusRes['jobList'] is empty to begin with.
        ## And statusRes['jobList'] may be empty even if jobs were created and have well
        ## defined state. An example is when the task has status FAILED in the Task DB
        ## because of an exception in the TaskWorker.
        resubmitjobids = [jobid for jobstatus, jobid in statusRes['jobList'] if (jobstatus in self.failedList) or (jobids and force and jobstatus in self.successList)]
        if statusRes['jobList'] and not resubmitjobids:
            msg  = "There are no jobs to resubmit."
            msg += " Only jobs in status %s can be resubmitted." % (self.failedList)
            msg += " Jobs in status %s can also be resubmitted, but only if the jobid is specified and force = True." % (self.successList)
            raise ExecutionError(msg)
        ## If the user wants to resubmit a specific set of jobs ...
        if jobids:
            ## ... make the intersection between the "allowed" and "wanted" jobs.
            resubmitjobids = list(set(resubmitjobids) & set(jobids))
            ## Check if all the "wanted" jobs can be resubmitted. If not, fail the resubmission.
            if len(resubmitjobids) != len(jobids):
                requestedResub = list(set(jobids) - set(resubmitjobids))
                msg  = "CRAB3 server refused to resubmit the following jobs: %s." % (str(requestedResub))
                msg += " Only jobs in status %s can be resubmitted." % (self.failedList)
                msg += " Jobs in status %s can also be resubmitted, but only if the jobid is specified and force = True." % (self.successList)
                raise ExecutionError(msg) #return [{'result': msg}]
        self.logger.info("Jobs to resubmit: %s" % (resubmitjobids))
        ## If these parameters were not set in the resubmission request, give them the
        ## same values they had in the original task submission.
        if (siteblacklist is None) or (sitewhitelist is None) or (maxjobruntime is None) or (maxmemory is None) or (numcores is None) or (priority is None):
            ## origValues = [orig_siteblacklist, orig_sitewhitelist, orig_maxjobruntime, orig_maxmemory, orig_numcores, orig_priority]
            origValues = self.api.query(None, None, self.Task.GetResubmitParams_sql, taskname = workflow).next()
            if siteblacklist is None:
                siteblacklist = literal_eval(origValues[0])
            if sitewhitelist is None:
                sitewhitelist = literal_eval(origValues[1])
            if maxjobruntime is None:
                maxjobruntime = origValues[2]
            if maxmemory is None:
                maxmemory = origValues[3]
            if numcores is None:
                numcores = origValues[4]
            if priority is None:
                priority = origValues[5]
        ## These are the parameters that we want to writte down in the 'tm_arguments'
        ## column of the Tasks DB each time a resubmission is done.
        ## DagmanResubmitter will read these parameters and write them into the task ad.
        arguments = {'resubmit_jobids' : resubmitjobids,
                     'site_blacklist'  : siteblacklist,
                     'site_whitelist'  : sitewhitelist,
                     'maxjobruntime'   : maxjobruntime,
                     'maxmemory'       : maxmemory,
                     'numcores'        : numcores,
                     'priority'        : priority
                    }
        ## Change the 'tm_arguments' column of the Tasks DB for this task to contain the
        ## above parameters.
        self.api.modify(self.Task.SetArgumentsTask_sql, taskname = [workflow], arguments = [str(arguments)])
        ## Change the status of the task in the Tasks DB to RESUBMIT.
        self.api.modify(self.Task.SetStatusTask_sql, status = ["RESUBMIT"], taskname = [workflow])
        return [{'result': retmsg}]


    def _updateTaskStatus(self, workflow, status, jobsPerStatus):
        """
        Update the status of the task when it is finished.
        More details: if the status of the task is submitted => all the jobs are finished then taskStatus=COMPLETED
                                                             => all the jobs are finished or failed then taskStatus=FAILED
        """
        if status == 'SUBMITTED':
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

    @conn_handler(services=['centralconfig'])
    def kill(self, workflow, force, jobids, userdn, userproxy=None):
        """Request to Abort a workflow.

           :arg str workflow: a workflow name"""

        retmsg = "ok"
        self.logger.info("About to kill workflow: %s. Getting status first." % workflow)
        statusRes = self.status(workflow, userdn, userproxy)[0]

        args = {'ASOURL' : statusRes.get("ASOURL", "")}
        # Hm...
        dbSerializer = str

        if statusRes['status'] in ['SUBMITTED', 'KILLFAILED', 'FAILED']:
            killList = [jobid for jobstatus, jobid in statusRes['jobList'] if jobstatus not in self.successList]
            if jobids:
                #if the user wants to kill specific jobids make the intersection
                killList = list(set(killList) & set(jobids))
                #check if all the requested jobids can be resubmitted
                if len(killList) != len(jobids):
                    retmsg = "Cannot request kill for %s" % (set(jobids) - set(killList))
            if not killList:
                raise ExecutionError("There are no jobs to kill. Only jobs not in %s states can be killed" % self.successList)
            self.logger.info("Jobs to kill: %s" % killList)

            args.update({"killList": killList, "killAll": jobids==[]})
            self.api.modify(self.Task.SetStatusTask_sql, status = ["KILL"], taskname = [workflow])
            self.api.modify(self.Task.SetArgumentsTask_sql, taskname = [workflow], arguments = [dbSerializer(args)])
        elif statusRes['status'] == 'NEW':
            self.api.modify(self.Task.SetStatusTask_sql, status = ["KILLED"], taskname = [workflow])
        else:
            raise ExecutionError("You cannot kill a task if it is in the %s state" % statusRes['status'])

        return [{"result":retmsg}]

    def proceed(self, workflow):
        """Continue a task which was initialized with 'crab submit --dryrun'.

           :arg str workflow: a workflow name
        """
        row = self.Task.ID_tuple(*self.api.query(None, None, self.Task.ID_sql, taskname=workflow).next())
        if row.task_status != 'UPLOADED':
            msg = 'Can only proceed if task is in the UPLOADED state, but it is in the %s state.' % row.task_status
            raise ExecutionError(msg)
        else:
            self.api.modify(self.Task.SetDryRun_sql, taskname=[workflow], dry_run=['F'])
            self.api.modify(self.Task.SetStatusTask_sql, taskname=[workflow], status=['NEW'])

        return [{'result': 'ok'}]
