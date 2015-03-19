import os
import sys
import time
import json
import errno
import classad
import logging

from ApmonIf import ApmonIf

from TaskWorker.Actions.RetryJob import JOB_RETURN_CODES 


class PreJob:
    """
    Need a doc string here.
    """

    def __init__(self):
        """
        PreJob constructor.
        """
        self.task_ad = classad.ClassAd()
        ## Set a logger for the pre-job.
        self.logger = logging.getLogger()
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", \
                                      datefmt = "%a, %d %b %Y %H:%M:%S %Z(%z)")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False

    def calculate_retry(self, id, retry_num):
        """
        Calculate the retry number we're on.  The DAGMan retry number is not
        really what we want; it is the number of times the post-job runs until
        completion.  Rather, we want to report the number of times the pre-job
        has been run.

        As a second issue, we want the post-job to be run at least once per time
        the job is submitted.  So, fail the pre-job if need be.  This is useful
        if the dag was restarted before the post-job was run and after the job
        completed.

        We determine the job was submitted in a
        """
        fname = "retry_info/job.%d.txt" % id
        if os.path.exists(fname):
            try:
                with open(fname, "r") as fd:
                    retry_info = json.load(fd)
            except:
                return retry_num
        else:
            retry_info = {"pre": 0, "post": 0}
        if 'pre' not in retry_info or 'post' not in retry_info:
            return retry_num
        retry_num = retry_info['post']

        out_fname = "job_out.%d" % id
        if retry_info['pre'] > retry_info['post']:
            # If job_out exists, then the job was likely submitted and we
            # should run post-job.
            if os.path.exists(out_fname):
                sys.exit(1)
        if (retry_info['pre'] <= retry_info['post']) and not os.path.exists(out_fname):
            retry_info['pre'] = retry_info['post'] + 1
        try:
            with open(fname + ".tmp", "w") as fd:
                json.dump(retry_info, fd)
            os.rename(fname + ".tmp", fname)
        except:
            return retry_num
        return retry_num


    def update_dashboard(self, retry, id, reqname, backend):
        """
        Need a doc string here.
        """
        if not self.task_ad:
            return
        params = {'tool': 'crab3',
                  'SubmissionType':'crab3',
                  'JSToolVersion': '3.3.0',
                  'tool_ui': os.environ.get('HOSTNAME',''),
                  'scheduler': 'GLIDEIN',
                  'GridName': self.task_ad['CRAB_UserDN'],
                  'ApplicationVersion': self.task_ad['CRAB_JobSW'],
                  'taskType': self.task_ad.get("CRAB_DashboardTaskType", 'analysistest'),
                  'vo': 'cms',
                  'CMSUser': self.task_ad['CRAB_UserHN'],
                  'user': self.task_ad['CRAB_UserHN'],
                  'taskId': self.task_ad['CRAB_ReqName'],
                  'datasetFull': self.task_ad['CRAB_InputData'],
                  'resubmitter': self.task_ad['CRAB_UserHN'],
                  'exe': 'cmsRun',
                  'jobId': ("%d_https://glidein.cern.ch/%d/%s_%d" % (id, id, self.task_ad['CRAB_ReqName'].replace("_", ":"), retry)),
                  'sid': "https://glidein.cern.ch/%d/%s" % (id, self.task_ad['CRAB_ReqName'].replace("_", ":")),
                  'broker': backend,
                  'bossId': str(id),
                  'localId' : '',
                 }
        apmon = ApmonIf()
        self.logger.debug("Dashboard task info: %s" % str(params))
        apmon.sendToML(params)
        apmon.free()


    def get_task_ad(self):
        """
        Need a doc string here.
        """
        self.task_ad = {}
        try:
            self.task_ad = classad.parseOld(open(os.environ['_CONDOR_JOB_AD']))
        except:
            msg = "Got exception while trying to parse the job ad."
            self.logger.exception(msg)


    def get_statistics(self):
        """
        Need a doc string here.
        """
        results = {}
        try:
            for state in JOB_RETURN_CODES._fields:
                count = 0
                with open("task_statistics.%s" % (state)) as fd:
                    for line in fd:
                        count += 1
                results[state] = count
        except:
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
                with open("task_statistics.%s.%s" % (site, state)) as fd:
                    for line in fd:
                        count += 1
                results[state] = count
        except:
            return {}
        return results


    def calculate_blacklist(self):
        """
        Need a doc string here.
        """
        # TODO: before we can do this, we need a more reliable way to pass
        # the site list to the prejob.
        return []


    def alter_submit(self, retry, id):
        new_submit_text = '+CRAB_Retry = %d\n' % retry
        if 'JobPrio' in self.task_ad:
            if id <= 5:
                self.task_ad['JobPrio'] += 10
            new_submit_text += '+JobPrio = %s\n' % str(self.task_ad['JobPrio'] + retry)
        if 'MaxWallTimeMins' in self.task_ad:
            new_submit_text += '+MaxWallTimeMins = %s\n' % str(self.task_ad.lookup('MaxWallTimeMins'))
        if 'RequestCpus' in self.task_ad:
            new_submit_text += '+RequestCpus = %s\n' % str(self.task_ad.lookup('RequestCpus'))
        if 'RequestMemory' in self.task_ad:
            new_submit_text += '+RequestMemory = %s\n' % str(self.task_ad.lookup('RequestMemory'))
        new_submit_text += '+PostJobPrio1 = -%s\n' % str(self.task_ad.lookup('QDate'))
        blacklist = self.calculate_blacklist()
        if blacklist:
            self.task_ad['CRAB_SiteAutomaticBlacklist'] = blacklist
            new_submit_text += '+CRAB_SiteAutomaticBlacklist = %s\n' % str(self.task_ad.lookup('CRAB_SiteAutomaticBlacklist'))
        with open("Job.submit", "r") as fd:
            new_submit_text += fd.read()

        new_submit_text = self.redo_sites(new_submit_text, id, blacklist)

        with open("Job.%d.submit" % id, "w") as fd:
            fd.write(new_submit_text)


    def redo_sites(self, new_submit_file, id, automatic_blacklist):

        if os.path.exists("site.ad.json"):
            with open("site.ad.json") as fd:
                site_info = json.load(fd)
            group = site_info[str(id)]
            available = set(site_info['groups'][str(group)])
        else:
            with open("site.ad") as fd:
                site_ad = classad.parse(fd)
            available = set(site_ad['Job%d' % id])

        blacklist = set(self.task_ad['CRAB_SiteBlacklist'])
        blacklist.update(automatic_blacklist)
        whitelist = set(self.task_ad['CRAB_SiteWhitelist'])
        if 'CRAB_SiteResubmitWhitelist' in self.task_ad:
            whitelist.update(self.task_ad['CRAB_SiteResubmitWhitelist'])
        if 'CRAB_SiteResubmitBlacklist' in self.task_ad:
            blacklist.update(self.task_ad['CRAB_SiteResubmitBlacklist'])

        if whitelist:
            available &= whitelist
        # Never blacklist something on the whitelist
        available -= (blacklist-whitelist)

        new_submit_file = '+DESIRED_SITES="%s"\n%s' % (",".join(available), new_submit_file)
        return new_submit_file


    def touch_logs(self, retry_num, id):
        """
        Create the log web-shared directory for the task and create the
        job_out.<jobid>.<retry>.txt and postjob.<jobid>.<retry>.txt files
        with default messages.
        """
        try:
            reqname = self.task_ad['CRAB_ReqName']
            logpath = os.path.expanduser("~/%s" % reqname)
            try:
                os.makedirs(logpath)
            except OSError, oe:
                if oe.errno != errno.EEXIST:
                    msg = "Failed to create log web-shared directory %s" % (logpath)
                    self.logger.info(msg)
                    return
            fname = os.path.join(logpath, "job_out.%s.%s.txt" % (id , retry_num))
            with open(fname, "w") as fd:
                fd.write("Job output has not been processed by post-job\n")
            fname = os.path.join(logpath, "postjob.%s.%s.txt" % (id, retry_num))
            with open(fname, "w") as fd:
                fd.write("Post-job is currently queued.\n")
            if retry_num:
                return time.time() - os.stat(os.path.join(logpath, "postjob.%s.%s.txt" % (id, int(retry_num)-1))).st_mtime
        except:
            msg = "Exception executing touch_logs()."
            self.logger.exception(msg)
            pass

    def execute(self, *args):
        """
        Need a doc string here.
        """
        retry_num = int(args[0])
        crab_id = int(args[1])
        retry_num = self.calculate_retry(crab_id, retry_num)
        reqname = args[2]
        backend = args[3]

        ## Create a directory in the schedd where to store the prejob logs.
        logpath = os.path.join(os.getcwd(), "prejob_logs")
        try:
            os.makedirs(logpath)
        except OSError, ose:
            if ose.errno != errno.EEXIST:
                logpath = os.getcwd()
        ## Create (open) the pre-job log file prejob.<job_id>.<crab_retry>.txt.
        prejob_log_file_name = os.path.join(logpath, "prejob.%d.%d.txt" % (crab_id, retry_num))
        fd_prejob_log = os.open(prejob_log_file_name, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0644)
        os.chmod(prejob_log_file_name, 0644)
        ## Redirect stdout and stderr to the pre-job log file.
        if os.environ.get('TEST_DONT_REDIRECT_STDOUT', False):
            print "Pre-job started with no output redirection."
        else:
            os.dup2(fd_prejob_log, 1)
            os.dup2(fd_prejob_log, 2)
            msg = "Pre-job started with output redirected to %s" % (prejob_log_file_name)
            self.logger.info(msg)

        self.get_task_ad()
        try:
            self.alter_submit(retry_num, crab_id)
        except:
            msg = "Exception executing the pre-job."
            self.logger.exception(msg)
            raise

        old_time = self.touch_logs(retry_num, crab_id)
        ## Note the cooloff time is based on the DAGMan retry number (i.e. the number of
        ## times the full cycle pre-job + job + post-job finished). This way, we don't
        ## punish users for condor re-starts.
        sleep_time = int(args[0])*60
        if old_time:
            sleep_time = int(max(1, sleep_time - old_time))
        if retry_num != 0:
            self.update_dashboard(retry_num, crab_id, reqname, backend)
        msg = "Finished pre-job execution. Sleeping %s seconds..." % (sleep_time)
        self.logger.info(msg)
        os.execv("/bin/sleep", ["sleep", str(sleep_time)])

