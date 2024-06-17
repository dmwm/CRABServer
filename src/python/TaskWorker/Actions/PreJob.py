from __future__ import print_function
import os
import sys
import time
import json
import errno
import classad
import logging
import htcondor
from ast import literal_eval

from ServerUtilities import getWebdirForDb, insertJobIdSid
from TaskWorker.Actions.RetryJob import JOB_RETURN_CODES

import CMSGroupMapper

class PreJob:
    """
    Need a doc string here.
    """
    def __init__(self):
        """
        PreJob constructor.
        """
        self.dag_retry     = None
        self.job_id        = None
        self.taskname      = None
        self.backend       = None
        self.stage         = None
        self.task_ad       = classad.ClassAd()
        self.userWebDirPrx = ""
        self.resubmit_info = {}
        self.prejob_exit_code = None
        ## Set a logger for the pre-job.
        self.logger = logging.getLogger()
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", \
                                      datefmt = "%a, %d %b %Y %H:%M:%S %Z(%z)")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False


    def calculate_crab_retry(self):
        """
        Calculate the job retry number we are on. The DAGMan retry number is not really
        what we want; this is the number of times the cycle [pre-job + job + post-job]
        ran until completion. Rather, we want to report the number of times the post-job
        has ran no matter if until completion or not. We call this the CRAB retry.
        As a second issue, we want the post-job to be run at least once per time the job
        is submitted. So, fail the pre-job if need be. This is useful if the dag was
        restarted before the post-job was run and after the job completed.
        """
        retmsg = ""
        ## Load the retry_info.
        retry_info_file_name = "retry_info/job.%s.txt" % (self.job_id)
        if os.path.exists(retry_info_file_name):
            try:
                with open(retry_info_file_name, 'r', encoding='utf-8') as fd:
                    retry_info = json.load(fd)
            except Exception:
                retmsg += "\n\tFailed to load file %s" % (retry_info_file_name)
                retmsg += "\n\tWill use DAGMan retry number (%s)" % (self.dag_retry)
                return self.dag_retry, retmsg
        else:
            retry_info = {'pre': 0, 'post': 0}

        retmsg += "\n\tLoaded retry_info = %s" % (retry_info)

        ## Simple validation of the retry_info.
        for key in ['pre',  'post']:
            if key not in retry_info:
                retmsg += "\n\tKey '%s' not found in retry_info (%s)" % (key, retry_info)
                retmsg += "\n\tWill use DAGMan retry number (%s)" % (self.dag_retry)
                return self.dag_retry, retmsg

        ## Define the retry number for the pre-job as the number of times the post-job has
        ## been ran.
        crab_retry = retry_info['post']

        ## The CRAB retry should never be smaller than the DAG retry by definition, as the
        ## CRAB retry counts what the DAG retry plus the post-job restarts. The reason why
        ## it can be crab_retry < dag_retry here is because the post-job failed to update
        ## the 'post' count in the retry_info dictionary.
        if crab_retry < self.dag_retry:
            retmsg += "\n\tWarning: CRAB retry (= %d) < DAG retry (= %d)." % (crab_retry, self.dag_retry)
            retmsg += " Will set CRAB Retry = DAG retry = %d." % (self.dag_retry)
            retry_info['post'] += self.dag_retry - crab_retry
            retry_info['pre']  += self.dag_retry - crab_retry
            retmsg += "\n\tUpdated retry_info = %s" % (retry_info)
            crab_retry = self.dag_retry

        ## The next (first) if statement is trying to catch the case in which the job or
        ## the pre-job was re-started before the post-job started to run ...
        job_out_file_name = "job_out.%s" % (self.job_id)
        if retry_info['pre'] > retry_info['post']:
            ## If job_out exists, then the job was likely submitted and we should run the
            ## post-job.
            if os.path.exists(job_out_file_name):
                retmsg += "\n\tFile %s already exists." % (job_out_file_name)
                retmsg += "\n\tIt seems the job has already been submitted."
                retmsg += "\n\tSetting the pre-job exit code to 1."
                self.prejob_exit_code = 1
        ## ... or not.
        else:
            ## If the job_out doesn't exist, then this is certainly (ok, 99.99% certainly)
            ## the first time the pre-job runs for this job retry. So we set the 'pre' count
            ## in retry_info equal to the 'post' count, and add +1 to account for the current
            ## pre-job run. Note that we don't use the 'pre' count anywhere else than here,
            ## so its only purpose is to detect job (pre-job) re-starts that occur before
            ## the post-job runs.
            if not os.path.exists(job_out_file_name):
                retry_info['pre'] = retry_info['post'] + 1
                retmsg += "\n\tUpdated retry_info = %s" % (retry_info)

        ## Save the retry_info dictionary to file.
        retmsg += "\n\tSaving retry_info = %s to %s" % (retry_info, retry_info_file_name)
        try:
            with open(retry_info_file_name + ".tmp", 'w', encoding='utf-8') as fd:
                json.dump(retry_info, fd)
            os.rename(retry_info_file_name + ".tmp", retry_info_file_name)
            retmsg += "\n\tSuccessfully saved %s" % (retry_info_file_name)
        except Exception:
            retmsg += "\n\tFailed to save %s" % (retry_info_file_name)

        return crab_retry, retmsg


    def update_dashboard(self, crab_retry):
        """
        Need a doc string here.
        """
        if not self.task_ad:
            return
        params = {'tool': 'crab3',
                  'SubmissionType': 'crab3',
                  'JSToolVersion': '3.3.0',
                  'tool_ui': os.environ.get('HOSTNAME', ''),
                  'scheduler': 'GLIDEIN',
                  'GridName': self.task_ad['CRAB_UserDN'],
                  'ApplicationVersion': self.task_ad['CRAB_JobSW'],
                  'taskType': self.task_ad.get("CRAB_DashboardTaskType", 'analysistest'),
                  'vo': 'cms',
                  'CMSUser': self.task_ad['CRAB_UserHN'],
                  'user': self.task_ad['CRAB_UserHN'],
                  'taskId': self.task_ad['CRAB_ReqName'],
                  'datasetFull': self.task_ad['DESIRED_CMSDataset'],
                  'resubmitter': self.task_ad['CRAB_UserHN'],
                  'exe': 'cmsRun',
                  'broker': self.backend,
                  'bossId': str(self.job_id),
                  'localId': '',
                  'SyncGridJobId': 'https://glidein.cern.ch/%s/%s' % (self.job_id, self.task_ad['CRAB_ReqName'].replace("_", ":")),
                 }

        if not self.userWebDirPrx:
            storage_rules = htcondor.param['CRAB_StorageRules']
            self.userWebDirPrx = getWebdirForDb(str(self.task_ad.get('CRAB_ReqName')), storage_rules)

        self.logger.info("User web dir: %s", self.userWebDirPrx)

        insertJobIdSid(params, self.job_id, self.task_ad['CRAB_ReqName'], crab_retry)

    def get_task_ad(self):
        """
        Need a doc string here.
        """
        self.task_ad = {}
        try:
            self.logger.info("Loading classads from: %s", os.environ['_CONDOR_JOB_AD'])
            self.task_ad = classad.parseOne(open(os.environ['_CONDOR_JOB_AD'], 'r', encoding='utf-8'))
            self.logger.info(str(self.task_ad))
        except Exception:
            msg = "Got exception while trying to parse the job ad."
            self.logger.exception(msg)


    def get_resubmit_info(self):
        """
        Need a doc string here.
        """
        file_name = "resubmit_info/job.%s.txt" % (self.job_id)
        if os.path.exists(file_name):
            with open(file_name, 'r', encoding='utf-8') as fd:
                self.resubmit_info = literal_eval(fd.read())


    def save_resubmit_info(self):
        """
        Need a doc string here.
        """
        file_name = "resubmit_info/job.%s.txt" % (self.job_id)
        with open(file_name + ".tmp", 'w', encoding='utf-8') as fd:
            fd.write(str(self.resubmit_info))
        os.rename(file_name + ".tmp", file_name)


    def get_statistics(self):
        """
        Need a doc string here.
        """
        results = {}
        try:
            for state in JOB_RETURN_CODES._fields:
                count = 0
                with open("task_statistics.%s" % (state), 'r', encoding='utf-8') as fd:
                    count = len(fd.read().split(b'\n')) - 1
                results[state] = count
        except Exception:
            return {}
        return results


    def get_site_statistics(self, site):
        """
        Need a doc string here.
        """
        results = {}
        try:
            for state in JOB_RETURN_CODES._fields:
                count = 0
                with open("task_statistics.%s.%s" % (site, state), 'r', encoding='utf-8') as fd:
                    count = len(fd.read().split(b'\n')) - 1
                results[state] = count
        except Exception:
            return {}
        return results


    def calculate_blacklist(self):
        """
        Need a doc string here.
        """
        # TODO: before we can do this, we need a more reliable way to pass
        # the site list to the prejob.
        return []


    def alter_submit(self, crab_retry):
        """
        Copy the content of the generic file Job.submit into a job-specific file
        Job.<job_id>.submit and add attributes that are job-specific (e.g. CRAB_Retry).
        Add also parameters that can be overwritten at each manual job resubmission
        (e.g. MaxWallTimeMins, RequestMemory, RequestCores, JobPrio, DESIRED_SITES).
        """
        ## Start the Job.<job_id>.submit content with the CRAB_Retry.
        new_submit_text = '+CRAB_Retry = %d\n' % (crab_retry)
        msg = "Setting CRAB_Retry = %s" % (crab_retry)
        self.logger.info(msg)
        ## Add job and postjob log URLs
        job_retry = "%s.%s" % (self.job_id, crab_retry)
        new_submit_text += '+CRAB_JobLogURL = %s\n' % classad.quote(os.path.join(self.userWebDirPrx, "job_out."+job_retry+".txt"))
        new_submit_text += '+CRAB_PostJobLogURL = %s\n' % classad.quote(os.path.join(self.userWebDirPrx, "postjob."+job_retry+".txt"))
        ## For the parameters that can be overwritten at each manual job resubmission,
        ## read them from the task ad, unless there is resubmission information there
        ## and this job is not one that has to be resubmitted, in which case we should
        ## use the same parameters (site black- and whitelists, requested memory, etc)
        ## as used by the previous job retry (which are saved in self.resubmit_info).
        CRAB_ResubmitList_in_taskad = ('CRAB_ResubmitList' in self.task_ad)
        use_resubmit_info = False
        resubmit_jobids = []
        if 'CRAB_ResubmitList' in self.task_ad:
            resubmit_jobids = map(str, self.task_ad['CRAB_ResubmitList'])
            try:
                resubmit_jobids = set(resubmit_jobids)
                if resubmit_jobids and self.job_id not in resubmit_jobids:
                    use_resubmit_info = True
            except TypeError:
                resubmit_jobids = True
        ## If there is no resubmit_info, we can of course not use it.
        if not self.resubmit_info:
            use_resubmit_info = False
        ## Get the resubmission parameters.
        maxjobruntime = None
        maxmemory     = None
        numcores      = None
        priority      = None
        if not use_resubmit_info:
            #if 'MaxWallTimeMins_RAW' in self.task_ad:
            #    if self.task_ad['MaxWallTimeMins_RAW'] != 1315:
            #        maxjobruntime = self.task_ad.lookup('MaxWallTimeMins_RAW')
            #        self.resubmit_info['maxjobruntime'] = maxjobruntime
            if 'MaxWallTimeMinsProbe' in self.task_ad and self.stage == 'probe':
                maxjobruntime = int(str(self.task_ad.lookup('MaxWallTimeMinsProbe')))
            elif 'MaxWallTimeMinsTail' in self.task_ad and self.stage == 'tail':
                maxjobruntime = int(str(self.task_ad.lookup('MaxWallTimeMinsTail')))
            elif 'MaxWallTimeMinsRun' in self.task_ad:
                maxjobruntime = int(str(self.task_ad.lookup('MaxWallTimeMinsRun')))
            if 'CRAB_RequestedMemory' in self.task_ad:
                maxmemory = int(str(self.task_ad.lookup('CRAB_RequestedMemory')))
            if 'CRAB_RequestedCores' in self.task_ad:
                numcores = int(str(self.task_ad.lookup('CRAB_RequestedCores')))
            if 'JobPrio' in self.task_ad:
                priority = int(str(self.task_ad['JobPrio']))
            if str(self.job_id) == '0': #jobids can be like 1-1 for subjobs
                priority = 20 #the maximum for splitting jobs
        else:
            inkey = str(crab_retry) if crab_retry == 0 else str(crab_retry - 1)
            while inkey not in self.resubmit_info and int(inkey) > 0:
                inkey = str(int(inkey) -  1)
            maxjobruntime = self.resubmit_info[inkey].get('maxjobruntime')
            maxmemory     = self.resubmit_info[inkey].get('maxmemory')
            numcores      = self.resubmit_info[inkey].get('numcores')
            priority      = self.resubmit_info[inkey].get('priority')
        ## Save the (new) values of the resubmission parameters in self.resubmit_info
        ## for the current job retry number.
        outkey = str(crab_retry)
        if outkey not in self.resubmit_info:
            self.resubmit_info[outkey] = {}
        self.resubmit_info[outkey]['maxjobruntime'] = maxjobruntime
        self.resubmit_info[outkey]['maxmemory']     = maxmemory
        self.resubmit_info[outkey]['numcores']      = numcores
        self.resubmit_info[outkey]['priority']      = priority
        self.resubmit_info[outkey]['use_resubmit_info'] = use_resubmit_info
        self.resubmit_info[outkey]['CRAB_ResubmitList_in_taskad'] = CRAB_ResubmitList_in_taskad

        ## Add the resubmission parameters to the Job.<job_id>.submit content.
        savelogs = 0 if self.stage == 'probe' else self.task_ad.lookup('CRAB_SaveLogsFlag')
        saveoutputs = 0 if self.stage == 'probe' else self.task_ad.lookup('CRAB_TransferOutputs')
        new_submit_text += '+CRAB_TransferOutputs = {0}\n+CRAB_SaveLogsFlag = {1}\n'.format(saveoutputs, savelogs)
        if maxjobruntime is not None:
            new_submit_text += '+EstimatedWallTimeMins = %s\n' % str(maxjobruntime)
            new_submit_text += '+MaxWallTimeMinsRun = %s\n' % str(maxjobruntime)  # how long it can run
            new_submit_text += '+MaxWallTimeMins = %s\n' % str(maxjobruntime)     # how long a slot can it match to
        # no plus sign for next 3 attributes, since those are Condor standard ones
        if maxmemory is not None:
            new_submit_text += 'RequestMemory = %s\n' % (str(maxmemory))
        if numcores is not None:
            new_submit_text += 'RequestCpus = %s\n' % (str(numcores))
        if priority is not None:
            new_submit_text += 'JobPrio = %s\n' % (str(priority))

        ## Within the schedd, order the first few jobs in the task before all other tasks of the same priority.
        pre_job_prio = 1
        if int(self.job_id.split('-')[0]) <= 5:
            pre_job_prio = 0
        new_submit_text += '+PreJobPrio1 = %d\n' % pre_job_prio

        ## The schedd will use PostJobPrio1 as a secondary job-priority sorting key: it
        ## will first run jobs by JobPrio; then, for jobs with the same JobPrio, it will
        ## run the job with the higher PostJobPrio1.
        new_submit_text += '+PostJobPrio1 = -%s\n' % str(self.task_ad.lookup('QDate'))

        ## Order retries before all other jobs in this task
        new_submit_text += '+PostJobPrio2 = %d\n' % crab_retry

        ## Add the site black- and whitelists and the DESIRED_SITES to the
        ## Job.<job_id>.submit content.
        new_submit_text = self.redo_sites(new_submit_text, crab_retry, use_resubmit_info)

        ## Add group information:
        username = self.task_ad.get('CRAB_UserHN')
        if 'CMSGroups' in self.task_ad:
            new_submit_text += '+CMSGroups = %s\n' % classad.quote(self.task_ad['CMSGroups'])
        elif username:
            groups = CMSGroupMapper.map_user_to_groups(username)
            if groups:
                new_submit_text += '+CMSGroups = %s\n' % classad.quote(groups)

        ## Finally add (copy) all the content of the generic Job.submit file.
        with open("Job.submit", 'r', encoding='utf-8') as fd:
            new_submit_text += fd.read()
        ## Write the Job.<job_id>.submit file.
        with open("Job.%s.submit" % (self.job_id), 'w', encoding='utf-8') as fd:
            fd.write(new_submit_text)


    def redo_sites(self, new_submit_text, crab_retry, use_resubmit_info):
        """
        Re-define the set of sites where the job can run on by taking into account
        any site-white-list and site-black-list.
        """
        ## If there is an automatic site blacklist, add it to the Job.<job_id>.submit
        ## content.
        automatic_siteblacklist = self.calculate_blacklist()
        if automatic_siteblacklist:
            self.task_ad['CRAB_SiteAutomaticBlacklist'] = automatic_siteblacklist
            new_submit_text += '+CRAB_SiteAutomaticBlacklist = %s\n' % str(self.task_ad.lookup('CRAB_SiteAutomaticBlacklist'))
        ## Get the site black- and whitelists either from the task ad or from
        ## self.resubmit_info.
        siteblacklist = set()
        sitewhitelist = set()
        if not use_resubmit_info:
            if 'CRAB_SiteBlacklist' in self.task_ad:
                siteblacklist = set(self.task_ad['CRAB_SiteBlacklist'])
            if 'CRAB_SiteWhitelist' in self.task_ad:
                sitewhitelist = set(self.task_ad['CRAB_SiteWhitelist'])
        else:
            inkey = str(crab_retry) if crab_retry == 0 else str(crab_retry - 1)
            while inkey not in self.resubmit_info and int(inkey) > 0:
                inkey = str(int(inkey) -  1)
            siteblacklist = set(self.resubmit_info[inkey].get('site_blacklist', []))
            sitewhitelist = set(self.resubmit_info[inkey].get('site_whitelist', []))
        ## Save the current site black- and whitelists in self.resubmit_info for the
        ## current job retry number.
        outkey = str(crab_retry)
        if outkey not in self.resubmit_info:
            self.resubmit_info[outkey] = {}
        self.resubmit_info[outkey]['site_blacklist'] = list(siteblacklist)
        self.resubmit_info[outkey]['site_whitelist'] = list(sitewhitelist)
        ## Add the current site black- and whitelists to the Job.<job_id>.submit
        ## content.
        if siteblacklist:
            new_submit_text += '+CRAB_SiteBlacklist = {"%s"}\n' % ('", "'.join(siteblacklist))
        else:
            new_submit_text += '+CRAB_SiteBlacklist = {}\n'
        if sitewhitelist:
            new_submit_text += '+CRAB_SiteWhitelist = {"%s"}\n' % ('", "'.join(sitewhitelist))
        else:
            new_submit_text += '+CRAB_SiteWhitelist = {}\n'
        ## Get the list of available sites (the sites where this job could run).
        if os.path.exists("site.ad.json"):
            with open("site.ad.json", 'r', encoding='utf-8') as fd:
                site_info = json.load(fd)
            group = site_info[self.job_id]
            available = set(site_info['group_sites'][str(group)])
            datasites = set(site_info['group_datasites'][str(group)])
        else:
            with open("site.ad", 'r', encoding='utf-8') as fd:
                site_ad = classad.parseOne(fd)
            available = set(site_ad['Job%s' % (self.job_id)])
        ## Take the intersection between the available sites and the site whitelist.
        ## This is the new set of available sites.
        if sitewhitelist:
            available &= sitewhitelist
        ## Remove from the available sites the ones that are in the site blacklist,
        ## unless they are also in the site whitelist (i.e. never blacklist something
        ## on the whitelist).
        siteblacklist.update(automatic_siteblacklist)
        available -= (siteblacklist - sitewhitelist)
        if not available:
            self.logger.error("Can not submit since DESIRED_Sites list is empty")
            self.prejob_exit_code = 1
            sys.exit(self.prejob_exit_code)
        ## Make sure that attributest which will be used in MatchMaking are SORTED lists
        available = list(available)
        available.sort()
        datasites = list(datasites)
        datasites.sort()
        ## Add DESIRED_SITES to the Job.<job_id>.submit content.
        new_submit_text = '+DESIRED_SITES="%s"\n%s' % (",".join(available), new_submit_text)
        new_submit_text = '+DESIRED_CMSDataLocations="%s"\n%s' % (",".join(datasites), new_submit_text)
        return new_submit_text

    def touch_logs(self, crab_retry):
        """
        Use the log web-shared directory created by AdjustSites.py for the task and create the
        job_out.<job_id>.<crab_retry>.txt and postjob.<job_id>.<crab_retry>.txt files
        with default messages.
        """
        try:
            taskname = self.task_ad['CRAB_ReqName']  # pylint: disable=unused-variable
            logpath = os.path.relpath('WEB_DIR')
            job_retry = "%s.%s" % (self.job_id, crab_retry)
            fname = os.path.join(logpath, "job_out.%s.txt" % job_retry)
            with open(fname, 'w', encoding='utf-8') as fd:
                fd.write("Job output has not been processed by post-job.\n")
            fname = "postjob.%s.txt" % job_retry
            with open(fname, 'w', encoding='utf-8') as fd:
                fd.write("Post-job is currently queued.\n")
            try:
                os.symlink(os.path.abspath(os.path.join(".", fname)), \
                           os.path.join(logpath, fname))
            except Exception:
                pass
            if crab_retry:
                return time.time() - os.stat(os.path.join(".", "postjob.%s.%s.txt" % (self.job_id, int(crab_retry)-1))).st_mtime
        except Exception:
            msg = "Exception executing touch_logs()."
            self.logger.exception(msg)


    def needsDefer(self):
        """ Check if the Prejob needs to be deferred, feature useful for some use cases that requires
            slow release of jobs in a task.
            The function return True if CRAB_JobReleaseTimeout is defined and not 0, and if the submit
            time of the task plus the defer time is greater than the current time.
        """
        deferTime = int(self.task_ad.get("CRAB_JobReleaseTimeout", 0))
        if deferTime:
            self.logger.info('Release timeout specified in extraJDL:')
            totalDefer = deferTime * int(self.job_id)
            submitTime = int(self.task_ad.get("CRAB_TaskSubmitTime"))
            currentTime = time.time()
            if currentTime < (submitTime + totalDefer):
                self.logger.info('  Defer time of this job (%s seconds since initial task submission) not elapsed yet, deferring for %s seconds', totalDefer, totalDefer)
                return True
            else:
                self.logger.info('  Continuing normally since current time is greater than requested starttime of the job')
        return False


    def execute(self, *args):
        """
        Need a doc string here.
        """
        self.dag_retry = int(args[0])
        self.job_id = str(args[1])
        self.taskname = args[2] # this is not used
        self.backend = args[3]
        self.stage = args[4]

        ## Calculate the CRAB retry count.
        crab_retry, calculate_crab_retry_msg = self.calculate_crab_retry()

        ## Create a directory in the schedd where to store the prejob logs.
        logpath = os.path.join(os.getcwd(), "prejob_logs")
        try:
            os.makedirs(logpath)
        except OSError as ose:
            if ose.errno != errno.EEXIST:
                logpath = os.getcwd()
        ## Create and open the pre-job log file prejob.<job_id>.<crab_retry>.txt.
        prejob_log_file_name = os.path.join(logpath, "prejob.%s.%s.txt" % (self.job_id, crab_retry))
        fd_prejob_log = open(prejob_log_file_name, 'w', encoding='utf-8')

        ## Redirect stdout and stderr to the pre-job log file.
        if os.environ.get('TEST_DONT_REDIRECT_STDOUT', False):
            print("Pre-job started with no output redirection.")
        else:
            os.dup2(fd_prejob_log, 1)
            os.dup2(fd_prejob_log, 2)
            msg = "Pre-job started with output redirected to %s" % (prejob_log_file_name)
            self.logger.info(msg)

        msg = "calculate_crab_retry returned the following message: %s" % (calculate_crab_retry_msg)
        self.logger.info(msg)

        ## If calculate_crab_retry() has set self.prejob_exit_code, we exit.
        if self.prejob_exit_code is not None:
            os.close(fd_prejob_log)
            sys.exit(self.prejob_exit_code)

        ## Load the task ad.
        self.get_task_ad()

        try:
            with open('webdir', 'r', encoding='utf-8') as fd:
                self.userWebDirPrx = fd.read()
        except IOError as e:
            self.logger.error("'I/O error(%s): %s', when looking for the 'webdir' file. Might be normal"
                              " if the schedd does not have a proxiedurl in the REST external config.", e.errno, e.strerror)

        try:
            self.get_resubmit_info()
            self.alter_submit(crab_retry)
            self.save_resubmit_info()
        except Exception:
            msg = "Exception executing the pre-job."
            self.logger.exception(msg)
            raise

        old_time = self.touch_logs(crab_retry)
        ## Note the cooloff time is based on the DAGMan retry number (i.e. the number of
        ## times the full cycle pre-job + job + post-job finished). This way, we don't
        ## punish users for condor re-starts.
        sleep_time = int(self.dag_retry)*60
        if old_time:
            sleep_time = int(max(1, sleep_time - old_time))
        self.update_dashboard(crab_retry)
        if self.needsDefer():
            return 4
        msg = "Finished pre-job execution. Sleeping %s seconds..." % (sleep_time)
        self.logger.info(msg)
        os.close(fd_prejob_log)
        os.execv("/bin/sleep", ["sleep", str(sleep_time)])
