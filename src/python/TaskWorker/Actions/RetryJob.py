import os
import re
import sys
import json
import shutil
import traceback
import subprocess
import classad
from collections import namedtuple


JOB_RETURN_CODES = namedtuple('JobReturnCodes', 'OK RECOVERABLE_ERROR FATAL_ERROR')(0, 1, 2)

# Fatal error limits for job resource usage
MAX_WALLTIME = 21*60*60 + 30*60
MAX_MEMORY = 2*1024

# Without this environment variable set, HTCondor takes a write lock per logfile entry
os.environ['_condor_ENABLE_USERLOG_LOCKING'] = 'false'

##==============================================================================

class FatalError(Exception):
    pass

class RecoverableError(Exception):
    pass

##==============================================================================

class RetryJob(object):
    """
    Need a doc string here.
    """
    def __init__(self):
        """
        Class constructor.
        """
        self.reqname             = None
        self.job_return_code     = None
        self.crab_retry          = None
        self.job_id              = None
        self.dag_jobid           = None
        self.site                = None
        self.ad                  = {}
        self.report              = {}
        self.validreport         = True
        self.integrated_job_time = 0

    ##= = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def get_aso_timeout(self):
        """
        Need a doc string here.
        """
        return min(max(self.integrated_job_time/4, 4*3600), 6*3600)

    ##= = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def get_job_ad(self):
        """
        Need a doc string here.
        """
        try:
            dag_clusterid = int(self.dag_jobid.split(".")[0])
            if dag_clusterid == -1:
                return
        except ValueError:
            pass

        shutil.copy("job_log", "job_log.%s" % str(self.dag_jobid))

        p = subprocess.Popen(["condor_q", "-debug", "-l", "-userlog", "job_log.%s" % str(self.dag_jobid), str(self.dag_jobid)], stdout=subprocess.PIPE, stderr=sys.stderr)
        output, _ = p.communicate()
        status = p.returncode

        try:
            os.unlink("job_log.%s" % str(self.dag_jobid))
        except:
            pass

        if status:
            raise FatalError("Failed to query condor user log:\n%s" % output)
        self.ads = []
        for text_ad in output.split("\n\n"):
            try:
                ad = classad.parseOld(text_ad)
            except SyntaxError:
                continue
            if ad:
                self.ads.append(ad)
        self.ad = self.ads[-1]
        if 'JOBGLIDEIN_CMSSite' in self.ad:
            self.site = self.ad['JOBGLIDEIN_CMSSite']

    ##= = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def get_report(self):
        """
        Need a doc string here.
        """
        try:
            with open("jobReport.json.%d" % (self.job_id), 'r') as fd:
                try:
                    self.report = json.load(fd)
                except ValueError:
                    self.report = {}
            site = self.report.get('executed_site', None)
            if site:
                self.site = site
        except IOError:
            self.validreport = False

    ##= = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def record_site(self, job_status):
        """
        Need a doc string here.
        """
        job_status_name = None
        for name, code in JOB_RETURN_CODES._asdict().iteritems():
            if code == job_status:
                job_status_name = name
        try:
            with os.fdopen(os.open("task_statistics.%s.%s" % (self.site, job_status_name), os.O_APPEND | os.O_CREAT | os.O_RDWR, 0644), 'a') as fd:
                fd.write("%d\n" % (self.job_id))
        except Exception as e:
            print "ERROR: %s" % str(e)
            # Swallow the exception - record_site is advisory only
        try:
            with os.fdopen(os.open("task_statistics.%s" % (job_status_name), os.O_APPEND | os.O_CREAT | os.O_RDWR, 0644), 'a') as fd:
                fd.write("%d\n" % (self.job_id))
        except Exception as exmsg:
            print "ERROR: %s" % (str(exmsg))
            # Swallow the exception - record_site is advisory only

    ##= = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def create_fake_fjr(self, exitMsg, exitCode):
        """If FatalError is got, fjr is not generated and it is needed for error_summary"""

        fake_fjr = {}
        fake_fjr['exitCode'] = exitCode
        fake_fjr['exitMsg'] = exitMsg
        jobReport = "job_fjr.%d.%d.json" % (self.job_id, self.crab_retry)
        if os.path.isfile(jobReport) and os.path.getsize(jobReport) > 0:
            #File exists and it is not empty
            msg  = "%s file exists and it is not empty!" % (jobReport)
            msg += " CRAB3 will overwrite it, because the job got FatalError"
            print msg
            with open(jobReport, 'r') as fd:
                print 'Old %s file content : ' % (jobReport)
                print fd.read()
        with open(jobReport, 'w') as fd:
            print  'New %s file content : %s' % (jobReport, json.dumps(fake_fjr))
            json.dump(fake_fjr, fd)

        # Fake FJR raises FatalError
        raise FatalError(exitMsg)

    ##= = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def check_cpu_report(self):
        """
        Need a doc string here.
        """
        # If job was killed on the worker node, we probably don't have a FJR.
        if self.ad.get("RemoveReason", "").startswith("Removed due to wall clock limit"):
            self.create_fake_fjr("Not retrying job due to wall clock limit (job automatically killed on the worker node)", 50664)
        subreport = self.report
        for attr in ['steps', 'cmsRun', 'performance', 'cpu', 'TotalJobTime']:
            subreport = subreport.get(attr, None)
            if subreport is None:
                return
        total_job_time = self.report['steps']['cmsRun']['performance']['cpu']['TotalJobTime']
        try:
            total_job_time = float(total_job_time)
        except ValueError:
            return
        integrated_job_time = 0
        for ad in self.ads:
            if 'RemoteWallClockTime' in ad:
                integrated_job_time += ad['RemoteWallClockTime']
        self.integrated_job_time = integrated_job_time
        # TODO: Compare the job against its requested walltime, not a hardcoded max.
        if total_job_time > MAX_WALLTIME:
            exitMsg = "Not retrying a long running job (job ran for %d hours)" % (total_job_time / 3600)
            self.create_fake_fjr(exitMsg, 50664)
        if integrated_job_time > 1.5*MAX_WALLTIME:
            exitMsg = "Not retrying a job because the integrated time (across all retries) is %d hours." % (integrated_job_time / 3600)
            self.create_fake_fjr(exitMsg, 50664)

    ##= = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def check_memory_report(self):
        """
        Need a doc string here.
        """
        # If job was killed on the worker node, we probably don't have a FJR.
        if self.ad.get("RemoveReason", "").startswith("Removed due to memory use"):
            self.create_fake_fjr("Not retrying job due to excessive memory use (job automatically killed on the worker node)", 50660)
        subreport = self.report
        for attr in ['steps', 'cmsRun', 'performance', 'memory', 'PeakValueRss']:
            subreport = subreport.get(attr, None)
            if subreport is None:
                return
        total_job_memory = self.report['steps']['cmsRun']['performance']['memory']['PeakValueRss']
        try:
            total_job_memory = float(total_job_memory)
        except ValueError:
            return
        # TODO: Compare the job against its requested memory, not a hardcoded max.
        if total_job_memory > MAX_MEMORY:
            exitMsg = "Not retrying job due to excessive memory use (%d MB)" % (total_job_memory)
            self.create_fake_fjr(exitMsg, 50660)

    ##= = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def check_disk_report(self):
        """
        Need a doc string here.
        """
        # If job was killed on the WN, we probably don't have a FJR.
        if self.ad.get("RemoveReason", "").startswith("Removed due to disk usage"):
            exitMsg = "Not retrying job due to excessive disk usage (job automatically killed on the worker node)"
            self.create_fake_fjr(exitMsg, 50662)

    ##= = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def check_exit_code(self):
        """
        Using the exit code saved in the json job report, decide whether it corresponds
        to a recoverable or a fatal error.
        """
        if 'exitCode' not in self.report:
            print "WARNING: 'exitCode' key not found in job report."
            return
        try:
            exitCode = int(self.report['exitCode'])
        except ValueError:
            print "WARNING: Unable to extract job's wrapper exit code from job report."
            return
        exitMsg = self.report.get("exitMsg", "UNKNOWN")

        if exitCode == 0:
            print "Job and stageout wrappers finished successfully (exit code %d)." % (exitCode)
            return

        msg  = "Job or stageout wrapper finished with exit code %d." % (exitCode)
        msg += " Trying to determine the meaning of the exit code and if it is a recoverable or fatal error."
        print msg

        ## Wrapper script sometimes returns the posix return code (8 bits).
        if exitCode in [8020, 8021, 8028] or exitCode in [84, 85, 92]:
            raise RecoverableError("Job failed to open local and fallback files.")

        if exitCode == 1:
            raise RecoverableError("Job failed to bootstrap CMSSW; likely a worker node issue.")

        if exitCode == 50513 or exitCode == 81:
            raise RecoverableError("Job did not find functioning CMSSW on worker node.")

        # This is a difficult one -- right now CMSRunAnalysis.py will turn things like
        # segfaults into an invalid FJR.  Will revisit this decision later.
        if exitCode == 50115 or exitCode == 195:
            raise RecoverableError("Job did not produce a FJR; will retry.")

        if exitCode == 134:
            recoverable_signal = False
            try:
                fname = os.path.expanduser("~/%s/job_out.%d.%d.txt" % (self.reqname, self.job_id, self.crab_retry))
                with open(fname) as fd:
                    for line in fd:
                        if line.startswith("== CMSSW:  A fatal system signal has occurred: illegal instruction"):
                            recoverable_signal = True
                            break
            except:
                print "Error analyzing abort signal."
                print str(traceback.format_exc())
            if recoverable_signal:
                raise RecoverableError("SIGILL; may indicate a worker node issue.")

        if exitCode == 8001 or exitCode == 65:
            cvmfs_issue = False
            try:
                fname = os.path.expanduser("~/%s/job_out.%d.%d.txt" % (self.reqname, self.job_id, self.crab_retry))
                cvmfs_issue_re = re.compile("== CMSSW:  unable to load /cvmfs/.*file too short")
                with open(fname) as fd:
                    for line in fd: 
                        if cvmfs_issue_re.match(line):
                            cvmfs_issue = True
                            break
            except:         
                print "Error analyzing output for CVMFS issues."
                print str(traceback.format_exc())
            if cvmfs_issue:
                raise RecoverableError("CVMFS issue detected.")

        # Another difficult case -- so far, SIGKILL has only been observed at T2_CH_CERN, and it has nothing to do
        # with an issue of the job itself.
        # We should revisit this issue if we see SIGKILL happening for other cases that are the users' fault.
        if exitCode == 137:
            raise RecoverableError("SIGKILL; likely an unrelated batch system kill.")

        if exitCode == 10034 or exitCode == 50:
            raise RecoverableError("Required application version not found at the site.")

        if exitCode == 60403 or exitCode == 243:
            raise RecoverableError("Timeout during attempted file stageout.")

        if exitCode == 60307 or exitCode == 147:
            raise RecoverableError("Error during attempted file stageout.")

        if exitCode == 60311 or exitCode == 151:
            raise RecoverableError("Error during attempted file stageout.")

        if exitCode:
            raise FatalError("Job wrapper finished with exit code %d.\nExit message:\n  %s" % (exitCode, exitMsg.replace('\n', '\n  ')))

    ##= = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
 
    def check_empty_report(self):
        """
        Need a doc string here.
        """
        if not self.report or not self.validreport:
            raise RecoverableError("Job did not produce a usable framework job report.")

    ##= = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def execute_internal(self, reqname, job_return_code, crab_retry, job_id, dag_jobid):
        """
        Need a doc string here.
        """
        self.reqname         = reqname
        self.job_return_code = job_return_code
        self.crab_retry      = crab_retry
        self.job_id          = job_id
        self.dag_jobid       = dag_jobid

        self.get_job_ad()
        self.get_report()

        if self.ad.get("RemoveReason", "").startswith("Removed due to job being held"):
            hold_reason = self.ad.get("HoldReason", self.ad.get("LastHoldReason", "Unknown"))
            raise RecoverableError("Will retry held job; last hold reason: %s" % (hold_reason))

        try:
            self.check_empty_report()
            ## Raises a RecoverableError or FatalError exception depending on the exitCode
            ## saved in the job report.
            self.check_exit_code()
        except RecoverableError as re:
            orig_msg = str(re)
            try:
                self.check_memory_report()
                self.check_cpu_report()
                self.check_disk_report()
            except:
                print "Original error: %s" % (orig_msg)
                raise
            raise

        if self.job_return_code != JOB_RETURN_CODES.OK: # Probably means stageout failed!
            raise RecoverableError("Payload job was successful, but wrapper exited with non-zero status %d (stageout failure)?" % (self.job_return_code))

        return JOB_RETURN_CODES.OK

    ##= = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def execute(self, *args, **kw):
        """
        Need a doc string here.
        """
        try:
            job_status = self.execute_internal(*args, **kw)
            self.record_site(job_status)
            return job_status
        except RecoverableError as remsg:
            print "%s" % (remsg)
            self.record_site(JOB_RETURN_CODES.RECOVERABLE_ERROR)
            return JOB_RETURN_CODES.RECOVERABLE_ERROR
        except FatalError as femsg:
            print "%s" % (femsg)
            self.record_site(JOB_RETURN_CODES.FATAL_ERROR)
            return JOB_RETURN_CODES.FATAL_ERROR
        except Exception:
            print str(traceback.format_exc())
            return 0 # Why do we return 0 here ?

