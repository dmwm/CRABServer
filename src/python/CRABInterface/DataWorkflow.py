import copy
import time
import logging
import datetime
from ast import literal_eval

## WMCore dependecies
from WMCore.REST.Error import ExecutionError
from WMCore.Database.CMSCouch import CouchServer

## CRAB dependencies
from ServerUtilities import checkTaskLifetime
from ServerUtilities import PUBLICATIONDB_STATUSES
from ServerUtilities import NUM_DAYS_FOR_RESUBMITDRAIN
from ServerUtilities import isCouchDBURL, getEpochFromDBTime

from CRABInterface.Utils import CMSSitesCache, conn_handler, getDBinstance


class DataWorkflow(object):
    """Entity that allows to operate on workflow resources.
       No aggregation of workflows provided here."""

    successList = ['finished']
    failedList = ['failed']

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

        self.splitArgMap = {
                        "Automatic": "minutes_per_job",
                        "LumiBased": "lumis_per_job",
                        "FileBased": "files_per_job",
                        "EventBased": "events_per_job",
                        "EventAwareLumiBased": "events_per_job"}

        self.Task = getDBinstance(config, 'TaskDB', 'Task')
        self.JobGroup = getDBinstance(config, 'TaskDB', 'JobGroup')
        self.FileMetaData = getDBinstance(config, 'FileMetaDataDB', 'FileMetaData')
        self.transferDB = getDBinstance(config, 'FileTransfersDB', 'FileTransfers')

    @classmethod
    def updateRequest(cls, workflow):
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

    @conn_handler(services=['centralconfig'])
    def submit(self, workflow, activity, jobtype, jobsw, jobarch, use_parent, secondarydata, generator, events_per_lumi, siteblacklist,
               sitewhitelist, splitalgo, algoargs, cachefilename, cacheurl, addoutputfiles,
               username, userdn, savelogsflag, publication, publishname, publishname2, asyncdest, dbsurl, publishdbsurl, vorole, vogroup, tfileoutfiles, edmoutfiles,
               runs, lumis, totalunits, adduserfiles, oneEventMode=False, maxjobruntime=None, numcores=None, maxmemory=None, priority=None, lfn=None,
               ignorelocality=None, saveoutput=None, faillimit=10, userfiles=None, userproxy=None, asourl=None, asodb=None, scriptexe=None, scriptargs=None,
               scheddname=None, extrajdl=None, collector=None, dryrun=False, publishgroupname=False, nonvaliddata=False, inputdata=None, primarydataset=None,
               debugfilename=None, submitipaddr=None, ignoreglobalblacklist=False):
        """Perform the workflow injection

           :arg str workflow: workflow name requested by the user;
           :arg str activity: workflow activity type, usually analysis;
           :arg str jobtype: job type of the workflow, usually Analysis;
           :arg str jobsw: software requirement;
           :arg str jobarch: software architecture (=SCRAM_ARCH);
           :arg str inputdata: input dataset;
           :arg str primarydataset: primary dataset;
           :arg str nonvaliddata: allow invalid input dataset;
           :arg int use_parent: add the parent dataset as secondary input;
           :arg str secondarydata: optional secondary dataset;
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
           :arg str username: username of the user doing the request;
           :arg int publication: flag enabling or disabling data publication;
           :arg str publishname: name to use for data publication; deprecated
           :arg str publishname: name to use for data publication;
           :arg str publishgroupname: add groupname or username to publishname;
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
           :arg str asodb: ASO db to be used in place of the one in the ext configuration. Default to asynctransfer.
           :arg str scheddname: Schedd Name used for debugging.
           :arg str collector: Collector Name used for debugging.
           :arg int dryrun: enable dry run mode (initialize but do not submit task).
           :returns: a dict which contaians details of the request"""

        backend_urls = copy.deepcopy(self.centralcfg.centralconfig.get("backend-urls", {}))
        if collector:
            backend_urls['htcondorPool'] = collector
        else:
            collector = backend_urls['htcondorPool']

        splitArgName = self.splitArgMap[splitalgo]
        dbSerializer = str

        ## If these parameters were not set in the submission request, give them
        ## predefined default values.
        if maxjobruntime is None:
            maxjobruntime = 1250
        if maxmemory is None:
            maxmemory = 2000
        if numcores is None:
            numcores = 1
        if priority is None:
            priority = 10

        arguments = {}

        ## Insert this new task into the Tasks DB.
        self.api.modify(self.Task.New_sql,
                            task_name       = [workflow],
                            task_activity   = [activity],
                            jobset_id       = [None],
                            task_status     = ['NEW'],
                            task_command    = ['SUBMIT'],
                            task_failure    = [''],
                            job_sw          = [jobsw],
                            job_arch        = [jobarch],
                            input_dataset   = [inputdata],
                            primary_dataset = [primarydataset],
                            nonvalid_data   = ['T' if nonvaliddata else 'F'],
                            use_parent      = [use_parent],
                            secondary_dataset = [secondarydata],
                            generator       = [generator],
                            events_per_lumi = [events_per_lumi],
                            site_whitelist  = [dbSerializer(sitewhitelist)],
                            site_blacklist  = [dbSerializer(siteblacklist)],
                            split_algo      = [splitalgo],
                            split_args      = [dbSerializer({'halt_job_on_file_boundaries': False, 'splitOnRun': False,
                                                splitArgName : algoargs, 'runs': runs, 'lumis': lumis})],
                            total_units     = [totalunits],
                            user_sandbox    = [cachefilename],
                            debug_files     = [debugfilename],
                            cache_url       = [cacheurl],
                            username        = [username],
                            user_dn         = [userdn],
                            user_vo         = ['cms'],
                            user_role       = [vorole],
                            user_group      = [vogroup],
                            publish_name    = [publishname2],
                            publish_groupname = ['T' if publishgroupname else 'F'],
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
                            asodb           = [asodb],
                            collector       = [collector],
                            schedd_name     = [scheddname],
                            dry_run         = ['T' if dryrun else 'F'],
                            user_files       = [dbSerializer(userfiles)],
                            transfer_outputs = ['T' if saveoutput else 'F'],
                            output_lfn       = [lfn],
                            ignore_locality  = ['T' if ignorelocality else 'F'],
                            fail_limit       = [faillimit],
                            one_event_mode   = ['T' if oneEventMode else 'F'],
                            submitter_ip_addr= [submitipaddr],
                            ignore_global_blacklist = ['T' if ignoreglobalblacklist else 'F']
        )

        return [{'RequestName': workflow}]

    def publicationStatusWrapper(self, workflow, asourl, asodb, username, publicationenabled):
        publicationInfo = {}
        if (publicationenabled == 'T'):
            #let's default asodb to asynctransfer, for old task this is empty!
            asodb = asodb or 'asynctransfer'
            publicationInfo = self.publicationStatus(workflow, asourl, asodb, username)
            self.logger.info("Publication status for workflow %s done", workflow)
        else:
            publicationInfo['status'] = {'disabled': []}
        return publicationInfo

    @conn_handler(services=['servercert'])
    def resubmit2(self, workflow, publication, jobids, siteblacklist, sitewhitelist, maxjobruntime, maxmemory,
                  numcores, priority, userproxy):
        """Request to reprocess what the workflow hasn't finished to reprocess.
           This needs to create a new workflow in the same campaign
        """
        retmsg = "ok"
        self.logger.info("Getting task ID tuple from DB for task %s", workflow)
        row = self.api.query(None, None, self.Task.ID_sql, taskname = workflow)
        try:
            #just one row is picked up by the previous query
            row = self.Task.ID_tuple(*next(row))
        except StopIteration:
            raise ExecutionError("Impossible to find task %s in the database." % workflow)

        submissionTime = getEpochFromDBTime(row.start_time)

        self.logger.info("Checking if resubmission is possible: we don't allow resubmission %s days before task expiration date", NUM_DAYS_FOR_RESUBMITDRAIN)
        retmsg = checkTaskLifetime(submissionTime)
        if retmsg != "ok":
            return [{'result': retmsg}]

        task_status = row.task_status
        task_splitting = row.split_algo

        resubmitWhat = "publications" if publication else "jobs"
        self.logger.info("About to resubmit %s for workflow: %s.", resubmitWhat, workflow)

        ## Ignore the following options if this is a publication resubmission or if the
        ## task was never submitted.
        if publication or task_status == 'SUBMITFAILED':
            jobids = None
            siteblacklist, sitewhitelist, maxjobruntime, maxmemory, numcores, priority = None, None, None, None, None, None

        # We only allow resubmission of tasks that are in a final state, listed here:
        allowedTaskStates = ['SUBMITTED', 'KILLED', 'KILLFAILED', 'RESUBMITFAILED', 'FAILED']

        # Do not resubmit publication for tasks that were not submitted since they don't have any output.
        if not publication:
            allowedTaskStates += ['SUBMITFAILED'] #NB submitfailed goes to NEW, not RESUBMIT
        ## If the task status is not an allowed one, fail the resubmission.
        if task_status not in allowedTaskStates:
            msg = "You cannot resubmit %s if the task is in status %s." % (resubmitWhat, task_status)
            raise ExecutionError(msg)

        if task_status == 'KILLED' and task_splitting == 'Automatic':
            msg = "You cannot resubmit {0} if the task is in status {1} and uses automatic splitting.".format(resubmitWhat, task_status)
            raise ExecutionError(msg)

        if task_status != 'SUBMITFAILED':
            if publication:
                ## Retrieve publication information.
                publicationEnabled = row.publication
                asourl = row.asourl
                asodb = row.asodb
                username = row.username
                publicationInfo = self.publicationStatusWrapper(workflow, asourl, asodb, username, publicationEnabled)

                if 'status' not in publicationInfo:
                    msg  = "Cannot resubmit publication."
                    msg += " Unable to retrieve the publication status."
                    raise ExecutionError(msg)
                if 'disabled' in publicationInfo:
                    msg  = "Cannot resubmit publication."
                    msg += " Publication was disabled in the CRAB configuration."
                    raise ExecutionError(msg)
                if 'error' in publicationInfo:
                    msg  = "Cannot resubmit publication."
                    msg += " Error in publication status: %s" % (publicationInfo['error'])
                    raise ExecutionError(msg)
                if isCouchDBURL(asourl) and publicationInfo['status'].get('publication_failed', 0) == 0:
                    msg = "There are no failed publications to resubmit."
                    raise ExecutionError(msg)
                ## Here we can add a check on the publication status of the documents
                ## corresponding to the job ids in resubmitjobids and jobids. So far the
                ## publication resubmission will resubmit all the failed publications.
                self.resubmitPublication(asourl, asodb, userproxy, workflow)
                return [{'result': retmsg}]
            else:
                self.logger.info("Jobs to resubmit: %s", jobids)

            ## If these parameters are not set, give them the same values they had in the
            ## original task submission.
            if (siteblacklist is None) or (sitewhitelist is None) or (maxjobruntime is None) or (maxmemory is None) or (numcores is None) or (priority is None):
                ## origValues = [orig_siteblacklist, orig_sitewhitelist, orig_maxjobruntime, orig_maxmemory, orig_numcores, orig_priority]
                origValues = next(self.api.query(None, None, self.Task.GetResubmitParams_sql, taskname = workflow))
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
            arguments = {'resubmit_jobids' : jobids,
                         'site_blacklist'  : siteblacklist,
                         'site_whitelist'  : sitewhitelist,
                         'maxjobruntime'   : maxjobruntime,
                         'maxmemory'       : maxmemory,
                         'numcores'        : numcores,
                         'priority'        : priority,
                         'resubmit_publication' : publication
                        }
            ## Change the 'tm_arguments' column of the Tasks DB for this task to contain the
            ## above parameters.
            self.api.modify(self.Task.SetArgumentsTask_sql, taskname = [workflow], arguments = [str(arguments)])

        ## Change the status of the task in the Tasks DB to RESUBMIT (or NEW).
        if task_status == 'SUBMITFAILED':
            newstate = ["NEW"]
            newcommand = ["SUBMIT"]
        else:
            newstate = ["NEW"]
            newcommand = ["RESUBMIT"]
        self.api.modify(self.Task.SetStatusTask_sql, status = newstate, command = newcommand, taskname = [workflow])
        return [{'result': retmsg}]

    def resubmit(self, workflow, publication, jobids, force, siteblacklist, sitewhitelist, maxjobruntime, maxmemory, numcores, priority, userdn, userproxy):
        """Request to reprocess what the workflow hasn't finished to reprocess.
           This needs to create a new workflow in the same campaign

           :arg str workflow: a valid workflow name
           :arg str list siteblacklist: black list of sites, with CMS name;
           :arg str list sitewhitelist: white list of sites, with CMS name;
           :arg int whether to resubmit publications or jobs."""
        retmsg = "ok"
        resubmitWhat = "publications" if publication else "jobs"

        self.logger.info("About to resubmit %s for workflow: %s. Getting status first.", resubmitWhat, workflow)

        ## Get the status of the task/jobs.
        statusRes = self.status(workflow, userdn, userproxy)[0]

        ## Check lifetime of the task and raise ExecutionError if appropriate
        self.logger.info("Checking if resubmission is possible: we don't allow resubmission %s days before task expiration date", NUM_DAYS_FOR_RESUBMITDRAIN)
        retmsg = checkTaskLifetime(statusRes['submissionTime'])
        if retmsg != "ok":
            return [{'result': retmsg}]

        ## Ignore the following options if this is a publication resubmission or if the
        ## task was never submitted.
        if publication or statusRes['status'] == 'SUBMITFAILED':
            jobids, force = None, False
            siteblacklist, sitewhitelist, maxjobruntime, maxmemory, numcores, priority = None, None, None, None, None, None

        ## We allow resubmission only if the task status is one of these:
        allowedTaskStates = ['SUBMITTED', 'KILLED', 'KILLFAILED', 'RESUBMITFAILED', 'FAILED']
        ## We allow resubmission of successfully finished jobs if the user explicitly
        ## gave the job ids and the force option (i.e. the user knows what he/she is
        ## doing). In that case we have to allow the task to be in COMPLETED status.
        ## The same is true for publication resubmission.
        if (jobids and force) or publication:
            allowedTaskStates += ['COMPLETED']
        ## Allow resubmission of tasks in SUBMITFAILED status if this is not a
        ## publication resubmission.
        if not publication:
            allowedTaskStates += ['SUBMITFAILED'] #NB submitfailed goes to NEW, not RESUBMIT
        ## If the task status is not an allowed one, fail the resubmission.
        if statusRes['status'] not in allowedTaskStates:
            if statusRes['status'] == 'COMPLETED':
                msg  = "Task status is COMPLETED."
                msg += " To resubmit jobs from a task in status COMPLETED, specify the job ids and use the force option."
                msg += " To resubmit publications use the publication option."
            else:
                msg = "You cannot resubmit %s if the task is in status %s." % (resubmitWhat, statusRes['status'])
            raise ExecutionError(msg)

        if statusRes['status'] != 'SUBMITFAILED':
            ## This is the list of job ids that we allow to be resubmitted.
            ## Note: This list will be empty if statusRes['jobList'] is empty to begin with.
            resubmitjobids = []
            for jobstatus, jobid in statusRes['jobList']:
                if (not publication and jobstatus in self.failedList) or \
                   (((jobids and force) or publication) and jobstatus in self.successList):
                    resubmitjobids.append(jobid)
            if statusRes['jobList'] and not resubmitjobids:
                msg = "There are no %s to resubmit." % (resubmitWhat)
                if publication:
                    msg += " Publications can only be resubmitted for jobs in status %s." % (self.successList)
                else:
                    msg += " Only jobs in status %s can be resubmitted." % (self.failedList)
                    msg += " Jobs in status %s can also be resubmitted," % (self.successList)
                    msg += " but only if the jobid is specified and the force option is set."
                raise ExecutionError(msg)
            ## Checks for publication resubmission.
            if publication:
                if 'publication' not in statusRes or not statusRes['publication']:
                    msg  = "Cannot resubmit publication."
                    msg += " Unable to retrieve the publication status."
                    raise ExecutionError(msg)
                if 'disabled' in statusRes['publication']:
                    msg  = "Cannot resubmit publication."
                    msg += " Publication was disabled in the CRAB configuration."
                    raise ExecutionError(msg)
                if 'error' in statusRes['publication']:
                    msg  = "Cannot resubmit publication."
                    msg += " Error in publication status: %s" % (statusRes['publication']['error'])
                    raise ExecutionError(msg)
                if isCouchDBURL(statusRes['ASOURL']) and statusRes['publication'].get('publication_failed', 0) == 0:
                    msg = "There are no failed publications to resubmit."
                    raise ExecutionError(msg)
                ## Here we can add a check on the publication status of the documents
                ## corresponding to the job ids in resubmitjobids and jobids. So far the
                ## publication resubmission will resubmit all the failed publications.

            ## If the user wants to resubmit a specific set of job ids ...
            if jobids:
                ## ... make the intersection between the "allowed" and "wanted" job ids.
                resubmitjobids = list(set(resubmitjobids) & set(jobids))
                ## Check if all the "wanted" job ids can be resubmitted. If not, fail the resubmission.
                if len(resubmitjobids) != len(jobids):
                    requestedResub = list(set(jobids) - set(resubmitjobids))
                    msg  = "CRAB server refused to resubmit the following jobs: %s." % (str(requestedResub))
                    msg += " Only jobs in status %s can be resubmitted." % (self.failedList)
                    msg += " Jobs in status %s can also be resubmitted," % (self.successList)
                    msg += " but only if the jobid is specified and the force option is set."
                    raise ExecutionError(msg) #return [{'result': msg}]
            if publication:
                self.logger.info("Publications to resubmit if failed: %s", resubmitjobids)
            else:
                self.logger.info("Jobs to resubmit: %s", resubmitjobids)

            ## If these parameters are not set, give them the same values they had in the
            ## original task submission.
            if (siteblacklist is None) or (sitewhitelist is None) or (maxjobruntime is None) or (maxmemory is None) or (numcores is None) or (priority is None):
                ## origValues = [orig_siteblacklist, orig_sitewhitelist, orig_maxjobruntime, orig_maxmemory, orig_numcores, orig_priority]
                origValues = next(self.api.query(None, None, self.Task.GetResubmitParams_sql, taskname = workflow))
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
                         'priority'        : priority,
                         'resubmit_publication' : publication
                        }
            ## Change the 'tm_arguments' column of the Tasks DB for this task to contain the
            ## above parameters.
            self.api.modify(self.Task.SetArgumentsTask_sql, taskname = [workflow], arguments = [str(arguments)])

        #TODO states are changed
        ## Change the status of the task in the Tasks DB to RESUBMIT (or NEW).
        if statusRes['status'] == 'SUBMITFAILED':
            newstate = ["NEW"]
            newcommand = ["SUBMIT"]
        else:
            newstate = ["NEW"]
            newcommand = ["RESUBMIT"]
        self.api.modify(self.Task.SetStatusTask_sql, status = newstate, command = newcommand, taskname = [workflow])
        return [{'result': retmsg}]


    def status(self, workflow, userdn, userproxy=None):
        """Retrieve the status of the workflow.

           :arg str workflow: a valid workflow name
           :return: a workflow status summary document"""
        raise NotImplementedError


    @conn_handler(services=['centralconfig'])
    def kill(self, workflow, force, jobids, killwarning, userdn, userproxy=None):
        """Request to Abort a workflow.

           :arg str workflow: a workflow name"""

        retmsg = "ok"
        self.logger.info("About to kill workflow: %s. Getting status first.", workflow)
        row = self.api.query(None, None, self.Task.ID_sql, taskname = workflow)
        try:
            #just one row is picked up by the previous query
            row = self.Task.ID_tuple(*next(row))
        except StopIteration:
            raise ExecutionError("Impossible to find task %s in the database." % workflow)
        warnings = literal_eval(row.task_warnings.read() if row.task_warnings else '[]') #there should actually is a default, but just in case
        if killwarning:
            warnings += [killwarning]
        warnings = str(warnings)

        args = {'ASOURL' : getattr(row, 'asourl', '')}

        if row.task_status in ['SUBMITTED', 'KILLFAILED', 'RESUBMITFAILED', 'FAILED', 'KILLED', 'TAPERECALL']:
            args.update({"killList": jobids})
            #Set arguments first so in case of failure we don't do any "damage"
            self.api.modify(self.Task.SetArgumentsTask_sql, taskname = [workflow], arguments = [str(args)])
            self.api.modify(self.Task.SetStatusWarningTask_sql, status = ["NEW"], command = ["KILL"], taskname = [workflow], warnings = [str(warnings)])
        elif row.task_status == 'NEW' and row.task_command == 'SUBMIT':
            #if the task has just been submitted and not acquired by the TW
            self.api.modify(self.Task.SetStatusWarningTask_sql, status = ["KILLED"], command = ["KILL"], taskname = [workflow], warnings = [str(warnings)])
        else:
            raise ExecutionError("You cannot kill a task if it is in the %s status" % row.task_status)

        return [{"result":retmsg}]


    def proceed(self, workflow):
        """Continue a task which was initialized with 'crab submit --dryrun'.

           :arg str workflow: a workflow name
        """
        row = self.Task.ID_tuple(*next(self.api.query(None, None, self.Task.ID_sql, taskname=workflow)))
        if row.task_status != 'UPLOADED':
            msg = 'Can only proceed if task is in the UPLOADED status, but it is in the %s status.' % row.task_status
            raise ExecutionError(msg)
        else:
            self.api.modify(self.Task.SetDryRun_sql, taskname=[workflow], dry_run=['F'])
            self.api.modify(self.Task.SetStatusTask_sql, taskname=[workflow], status=['NEW'], command=['SUBMIT'])

        return [{'result': 'ok'}]

    def resubmitPublication(self, asourl, asodb, proxy, taskname):
        if isCouchDBURL(asourl):
            return self.resubmitCouchPublication(asourl, asodb, proxy, taskname)
        else:
            return self.resubmitOraclePublication(taskname)

    def resubmitCouchPublication(self, asourl, asodb, proxy, taskname):
        """
        Resubmit failed publications by resetting the publication
        status in the CouchDB documents.
        """
        server = CouchServer(dburl=asourl, ckey=proxy, cert=proxy)
        try:
            database = server.connectDatabase(asodb)
        except Exception as ex:
            msg = "Error while trying to connect to CouchDB: %s" % (str(ex))
            raise Exception(msg)
        try:
            failedPublications = database.loadView('DBSPublisher', 'PublicationFailedByWorkflow', {'reduce': False, 'startkey': [taskname], 'endkey': [taskname, {}]})['rows']
        except Exception as ex:
            msg = "Error while trying to load view 'DBSPublisher.PublicationFailedByWorkflow' from CouchDB: %s" % (str(ex))
            raise Exception(msg)
        msg = "There are %d failed publications to resubmit: %s" % (len(failedPublications), failedPublications)
        self.logger.info(msg)
        for doc in failedPublications:
            docid = doc['id']
            if doc['key'][0] != taskname: # this should never happen...
                msg = "Skipping document %s as it seems to correspond to another task: %s" % (docid, doc['key'][0])
                self.logger.warning(msg)
                continue
            data = {'last_update': time.time(),
                    'retry': str(datetime.datetime.now()),
                    'publication_state': 'not_published',
                   }
            try:
                database.updateDocument(docid, 'DBSPublisher', 'updateFile', data)
                self.logger.info("updating document %s ", docid)
            except Exception as ex:
                self.logger.error("Error updating document %s in CouchDB: %s", docid, str(ex))
        return

    def resubmitOraclePublication(self, taskname):
        binds = {}
        binds['taskname'] = [taskname]
        binds['last_update'] = [int(time.time())]
        binds['publication_state'] = [PUBLICATIONDB_STATUSES['FAILED']]
        binds['new_publication_state'] = [PUBLICATIONDB_STATUSES['NEW']]
        self.api.modifynocheck(self.transferDB.RetryUserPublication_sql, **binds)
        return

