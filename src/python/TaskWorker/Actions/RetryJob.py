import os
import re
import sys
import json
import shutil
import subprocess
import socket
from collections import namedtuple

from ServerUtilities import executeCommand
from ServerUtilities import MAX_DISK_SPACE, MAX_WALLTIME, MAX_MEMORY

if 'useHtcV2' in os.environ:
    import classad2 as classad
else:
    import classad

JOB_RETURN_CODES = namedtuple('JobReturnCodes', 'OK RECOVERABLE_ERROR FATAL_ERROR')(0, 1, 2)

# Without this environment variable set, HTCondor takes a write lock per logfile entry
os.environ['_condor_ENABLE_USERLOG_LOCKING'] = 'false'

# ==============================================================================


class FatalError(Exception):
    pass


class RecoverableError(Exception):
    pass


# ==============================================================================

class RetryJob():
    """
    Need a doc string here.
    """
    def __init__(self):
        """
        Class constructor.
        """
        self.logger = None
        self.reqname = None
        self.job_return_code = None
        self.dag_retry = None
        self.crab_retry = None
        self.job_id = None
        self.dag_jobid = None
        self.dag_clusterid = None
        self.site = None
        self.username = None
        self.ads = []
        self.ad = {}
        self.report = {}
        self.validreport = True
        self.integrated_job_time = 0

        self.MAX_DISK_SPACE = MAX_DISK_SPACE
        self.MAX_WALLTIME = MAX_WALLTIME
        self.MAX_MEMORY = MAX_MEMORY

    # = = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def get_job_ad_from_condor_q(self):
        """
        Need a doc string here.
        """
        if self.dag_clusterid == -1:
            return
        shutil.copy("job_log", "job_log.%s" % str(self.dag_jobid))
        p = subprocess.Popen(["condor_q", "-debug", "-l", "-userlog", "job_log.%s" %
                              str(self.dag_jobid), str(self.dag_jobid)], stdout=subprocess.PIPE, stderr=sys.stderr)
        output, _ = p.communicate()
        status = p.returncode

        try:
            os.unlink("job_log.%s" % str(self.dag_jobid))
        except Exception:  # pylint: disable=broad-except
            pass

        if status:
            raise FatalError("Failed to query condor user log:\n%s" % output)

        for text_ad in output.split("\n\n"):
            try:
                ad = classad.parseOne(text_ad)
            except SyntaxError:
                continue
            if ad:
                self.ads.append(ad)
        self.ad = self.ads[-1]

    # = = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def get_job_ad_from_file(self):
        """
        Need a doc string here
        """
        self.ads.append(self.ad)
        if self.dag_retry == 0:
            msg = "This is job retry number 0. Will not try to search and load previous job ads."
            self.logger.info(msg)
            return
        for dag_retry in range(self.dag_retry):
            job_ad_file = os.path.join(".", "finished_jobs", "job.%s.%d" % (self.job_id, dag_retry))
            if os.path.isfile(job_ad_file):
                try:
                    with open(job_ad_file, encoding='utf-8') as fd:
                        ad = classad.parseOne(fd)
                except Exception:  # pylint: disable=broad-except
                    msg = "Unable to parse classads from file %s. Continuing." % (job_ad_file)
                    self.logger.warning(msg)
                    continue
                if ad:
                    self.ads.append(ad)
            else:
                msg = "File %s does not exist. Continuing." % (job_ad_file)
                self.logger.warning(msg)

    # = = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def get_report(self):
        """
        Need a doc string here.
        """
        try:
            with open("jobReport.json.%s" % (self.job_id), 'r', encoding='utf-8') as fd:
                try:
                    self.report = json.load(fd)
                except ValueError:
                    self.report = {}
            site = self.report.get('executed_site', None)
            if site:
                self.site = site
        except IOError:
            self.validreport = False

    # = = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def record_site(self, job_status):
        """
        Need a doc string here.
        """
        job_status_name = None
        for name, code in JOB_RETURN_CODES._asdict().items():
            if code == job_status:
                job_status_name = name
        try:
            with os.fdopen(os.open("task_statistics.%s.%s" % (self.site, job_status_name), os.O_APPEND | os.O_CREAT | os.O_RDWR, 0o644), 'a') as fd:
                fd.write("%s\n" % (self.job_id))
        except Exception as ex:  # pylint: disable=broad-except
            self.logger.error(str(ex))
            # Swallow the exception - record_site is advisory only
        try:
            with os.fdopen(os.open("task_statistics.%s" % (job_status_name), os.O_APPEND | os.O_CREAT | os.O_RDWR, 0o644), 'a') as fd:
                fd.write("%s\n" % (self.job_id))
        except Exception as ex:  # pylint: disable=broad-except
            self.logger.error(str(ex))
            # Swallow the exception - record_site is advisory only

    # = = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def create_fake_fjr(self, exitMsg, exitCode, jobExitCode=None, fatalError=True):
        """
        If FatalError is got, fjr is not generated and it is needed for error_summary
        Can also used to "fix" the exit code in the fjr yet allow a resubmission if fatalError is False
        """

        fake_fjr = {}
        fake_fjr['exitMsg'] = exitMsg
        if jobExitCode:
            fake_fjr['jobExitCode'] = jobExitCode
            fake_fjr['exitCode'] = jobExitCode
        else:
            fake_fjr['exitCode'] = exitCode
        jobReport = "job_fjr.%s.%d.json" % (self.job_id, self.crab_retry)
        if os.path.isfile(jobReport) and os.path.getsize(jobReport) > 0:
            # File exists and it is not empty
            msg = "%s file exists and it is not empty!" % (jobReport)
            msg += " CRAB3 will overwrite it, because the job got FatalError"
            self.logger.info(msg)
            with open(jobReport, 'r', encoding='utf-8') as fd:
                msg = "Old %s file content: %s" % (jobReport, fd.read())
                self.logger.info(msg)
        with open(jobReport, 'w', encoding='utf-8') as fd:
            msg = "New %s file content: %s" % (jobReport, json.dumps(fake_fjr))
            self.logger.info(msg)
            json.dump(fake_fjr, fd)

        # Fake FJR raises FatalError
        if fatalError:
            raise FatalError(exitMsg)

    # = = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def check_cpu_report(self):
        """
        Need a doc string here.
        """
        # If job was killed on the worker node, we probably don't have a FJR.
        if self.ad.get("RemoveReason", "").startswith("Removed due to wall clock limit"):
            exitMsg = "Not retrying job due to wall clock limit (job automatically killed on the worker node)"
            self.create_fake_fjr(exitMsg, 50664, 50664)  # this raises FatalError
        if "CPU usage over limit" in self.ad.get("RemoveReason", ""):
            exitMsg = "Not retrying job due CPU limit (using more CPU than Wall Clock)"
            self.create_fake_fjr(exitMsg, 50663, 50663)  # this raises FatalError
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
        if total_job_time > self.MAX_WALLTIME:
            exitMsg = "Not retrying a long running job (job ran for %d hours)" % (total_job_time / 3600)
            self.create_fake_fjr(exitMsg, 50664)  # this raises FatalError
        if integrated_job_time > (1.5 * self.MAX_WALLTIME):
            exitMsg = "Not retrying a job because the integrated time (across all retries) is %d hours." % (integrated_job_time / 3600)
            self.create_fake_fjr(exitMsg, 50664)  # this raises FatalError

    # = = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def check_memory_report(self):
        """
        Need a doc string here.
        """
        # If job was killed on the worker node, we probably don't have a FJR.
        if self.ad.get("RemoveReason", "").startswith("Removed due to memory use"):
            job_rss = int(self.ad.get("ResidentSetSize", "0")) // 1000
            exitMsg = "Job killed by HTCondor due to excessive memory use"
            exitMsg += " (RSS=%d MB)." % job_rss
            exitMsg += " Will not retry it."
            self.create_fake_fjr(exitMsg, 50660, 50660)
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
        if total_job_memory > self.MAX_MEMORY:
            exitMsg = "Not retrying job due to excessive memory use (%d MB vs %d MB requested)" % (total_job_memory, self.MAX_MEMORY)
            self.create_fake_fjr(exitMsg, 50660, 50660)

    # = = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def check_disk_report(self):
        """
        This function checks 2 things:
            a) If remove reason is 'Removed due to disk usage' which is set in PeriodicRemove
                  expression. PeriodicRemove & PeriodicRemoveReason is set in DagmanCreator.py
                  before task submittion to scheduler.
            b) If disk usage is >= of Maximum allowed disk usage.
        If one or other evaluates to true, it will create fake_fjr and job will not be retried.
        """
        # If job was killed on the WN, we probably don't have a FJR.
        if self.ad.get("RemoveReason", "").startswith("Removed due to disk usage"):
            exitMsg = "Not retrying job due to excessive disk usage (job automatically killed on the worker node)"
            self.create_fake_fjr(exitMsg, 50662, 50662)
        if 'DiskUsage' in self.ad:
            diskUsage = int(self.ad['DiskUsage'])
            if diskUsage >= self.MAX_DISK_SPACE:
                self.logger.debug("Disk Usage: %s, Maximum allowed disk usage: %s", diskUsage, self.MAX_DISK_SPACE)
                exitMsg = "Not retrying job due to excessive disk usage (job automatically killed on the worker node)"
                self.create_fake_fjr(exitMsg, 50662, 50662)
            else:
                msg = "Unable to get DiskUsage from job classads. Will not perform Disk Usage check."
                self.logger.debug(msg)

    # = = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def check_expired_report(self):
        """
        If a job was removed because it stay idle more than a week don't retry
        """
        if self.ad.get("RemoveReason", "").startswith("Removed due to idle time limit"):
            exitMsg = "Not retrying job due to excessive idle time (job automatically killed on the grid scheduler)"
            self.create_fake_fjr(exitMsg, 50665, 50665)

    # = = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def check_exit_code(self):
        """
        Using the exit code saved in the json job report, decide whether it corresponds
        to a recoverable or a fatal error.
        """
        if 'exitCode' not in self.report:
            msg = "'exitCode' key not found in job report."
            self.logger.warning(msg)
            return 1
        try:
            exitCode = int(self.report['exitCode'])
        except ValueError:
            msg = "Unable to extract job's wrapper exit code from job report."
            self.logger.warning(msg)
            return 1
        exitMsg = self.report.get("exitMsg", "UNKNOWN")

        if exitCode == 0:
            msg = "Job and stageout wrappers finished successfully (exit code %d)." % (exitCode)
            self.logger.info(msg)
            return 0

        msg = "Job or stageout wrapper finished with exit code %d." % (exitCode)
        msg += " Trying to determine the meaning of the exit code and if it is a recoverable or fatal error."
        self.logger.info(msg)

        # Wrapper script sometimes returns the posix return code (8 bits).
        if exitCode in [8020, 8021, 8022, 8028] or exitCode in [84, 85, 86, 92]:
            try:  # the following is still a bit experimental, make sure it never crashes the PJ
                corruptedInputFile = self.check_corrupted_file(exitCode)
            except Exception as e:  # pylint: disable=broad-except
                msg = f"check_corrupted_file raised an exception:\n{e}\nIgnore and go on"
                self.logger.error(msg)
                corruptedInputFile = False
            if corruptedInputFile:
                exitMsg = "Fatal Root Error maybe a corrupted input file. This error is being reported"
                self.create_fake_fjr(exitMsg, 8022, 8022, fatalError=False)  # retry the job
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
                fname = os.path.realpath("WEB_DIR/job_out.%s.%d.txt" % (self.job_id, self.crab_retry))
                with open(fname, encoding='utf-8') as fd:
                    for line in fd:
                        if line.startswith("== CMSSW:  A fatal system signal has occurred: illegal instruction"):
                            recoverable_signal = True
                            break
            except Exception:  # pylint: disable=broad-except
                msg = "Error analyzing abort signal."
                msg += "\nDetails follow:"
                self.logger.exception(msg)
            if recoverable_signal:
                raise RecoverableError("SIGILL; may indicate a worker node issue.")

        if exitCode == 8001 or exitCode == 65:
            cvmfs_issue = False
            try:
                fname = os.path.relpath("WEB_DIR/job_out.%s.%d.txt" % (self.job_id, self.crab_retry))
                cvmfs_issue_re = re.compile("== CMSSW:  unable to load /cvmfs/.*file too short")
                with open(fname, encoding='utf-8') as fd:
                    for line in fd:
                        if cvmfs_issue_re.match(line):
                            cvmfs_issue = True
                            break
            except Exception:  # pylint: disable=broad-except
                msg = "Error analyzing output for CVMFS issues."
                msg += "\nDetails follow:"
                self.logger.exception(msg)
            if cvmfs_issue:
                raise RecoverableError("CVMFS issue detected.")

        # Another difficult case -- so far, SIGKILL has mostly been observed at T2_CH_CERN, and it has nothing to do
        # with an issue of the job itself. Typically, this isn't the user code's fault
        # it was often a site or pilot misconfiguration that led to the pilot exhausting its allocated runtime.
        # We should revisit this issue if we see SIGKILL happening for other cases that are the users' fault.
        if exitCode == 137:
            raise RecoverableError("SIGKILL; likely an unrelated batch system kill.")

        if exitCode == 10034 or exitCode == 50:
            raise RecoverableError("Required application version not found at the site.")

        if exitCode == 10040:
            raise RecoverableError("Site Error: failed to generate cmsRun cfg file at runtime.")

        if exitCode == 60403 or exitCode == 243:
            raise RecoverableError("Timeout during attempted file stageout.")

        if exitCode == 60307 or exitCode == 147:
            raise RecoverableError("Error during attempted file stageout.")

        if exitCode == 60311 or exitCode == 151:
            raise RecoverableError("Error during attempted file stageout.")

        if exitCode:
            raise FatalError("Job wrapper finished with exit code %d.\nExit message:\n  %s" % (exitCode, exitMsg.replace('\n', '\n  ')))

        return 0

    # = = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def check_corrupted_file(self, exitCode):
        """
        check if job stdout contains a message indicating a corrupted file and reports this
        via a json file taskname.corrupted.job.<crabid>.<retry>.json
        returns True/False accordingly to corrupted yes/no
        """

        corruptedFile = False
        suspiciousFile = False
        inputFileName = 'NotAvailable'
        RSE = self.site
        RSE = RSE if not RSE.startswith('T1') else f"{RSE}_Disk"
        fname = os.path.realpath("WEB_DIR/job_out.%s.%d.txt" % (self.job_id, self.crab_retry))
        self.logger.debug(f'exit code {exitCode}, look for corrupted file in {fname}')
        with open(fname, encoding='utf-8') as fd:
            for line in fd:
                line = line.rstrip()  # remove trailing new-line
                # remember last opened file, in case of 8021 that's the one that matters
                if line.startswith("== CMSSW:") and ' Successfully opened file' in line:
                    inputFileName = f"/store/{line.split('/store/')[1]}"  # strip protocol part
                if line.startswith("== CMSSW:") and "Fatal Root Error:" in line:
                    corruptedFile = True
                    self.logger.info("Corrupted input file found")
                    self.logger.debug(line)
                    errorLines = [line]
                    # file name is in next line
                    continue
                if corruptedFile:
                    errorLines.append(line)
                    if '/store/' in line and '.root' in line:
                        # this may be better done in the script which processes the BadInputFiles reports
                        # if '/store/user' in line or '/store/group' in line and not 'rucio' in line:
                        #    # no point in reporting files unknown to Rucio
                        #    corruptedFile = False
                        #    break

                        # extract the '/store/...root' part of this line
                        fragment1 = line.split('/store/')[1]
                        fragment2 = fragment1.split('.root')[0]
                        inputFileName = f"/store/{fragment2}.root"
                        self.logger.info(f"RSE: {RSE} - ec: {exitCode} - file: {inputFileName}")

                    else:
                        corruptedFile = False
                        suspiciousFile = True
                        errorLines.append('NOT CLEARLY CORRUPTED, OTHER ROOT ERROR ?')
                        errorLines.append('DID Identification may not be correct')
                        self.logger.info("RootFatalError does not contain file info")
                    break
        if corruptedFile or suspiciousFile:
            # add pointers to logs
            schedHostname = socket.gethostname().split('.')[0]
            schedId = schedHostname.split('0')[1]  # vomcs059 --> 59, vocms0144 --> 144 etc,
            username = self.reqname.split(':')[1].split('_')[0]
            webDirUrl = f"https://cmsweb.cern.ch:8443/scheddmon/0{schedId}/{username}/{self.reqname}"
            stdoutUrl = f"{webDirUrl}/job_out.{self.job_id}.{self.crab_retry}.txt"
            postJobUrl = f"{webDirUrl}/postjob.{self.job_id}.{self.crab_retry}.txt"
            errorLines.append(f"stdout: {stdoutUrl}")
            errorLines.append(f"postjob: {postJobUrl}")
            # note things  down
            reportFileName = f'{self.reqname}.job.{self.job_id}.{self.crab_retry}.json'
            corruptionMessage = {'DID': f'cms:{inputFileName}', 'RSE': RSE,
                                 'exitCode': exitCode, 'message': errorLines}
            with open(reportFileName, 'w', encoding='utf-8') as fp:
                json.dump(corruptionMessage, fp)
            self.logger.info('corruption message prepared, gfal-copy to EOS')
            proxy = os.getenv('X509_USER_PROXY')
            self.logger.info(f"X509_USER_PROXY = {proxy}")
            if corruptedFile:
                reportLocation = 'davs://eoscms.cern.ch:443/eos/cms/store/temp/user/BadInputFiles/corrupted/new/'
            if suspiciousFile:
                reportLocation = 'davs://eoscms.cern.ch:443/eos/cms/store/temp/user/BadInputFiles/suspicious/new/'

            destination = reportLocation + reportFileName
            cmd = f'gfal-copy -vp -t 60 {reportFileName} {destination}'
            out, err, ec = executeCommand(cmd)
            if ec:
                self.logger.error(f'gfal-copy failed with out: {out} err: {err}')
        return corruptedFile

    # = = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def check_empty_report(self):
        """
        Need a doc string here.
        """
        if not self.report or not self.validreport:
            raise RecoverableError("Job did not produce a usable framework job report.")

    # = = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def execute_internal(self, logger, reqname, username, job_return_code, dag_retry, crab_retry, job_id, dag_jobid, job_ad, used_job_ad):
        """
        Need a doc string here.
        """
        self.logger = logger
        self.reqname = reqname
        self.username = username
        self.job_return_code = job_return_code
        self.dag_retry = dag_retry
        self.crab_retry = crab_retry
        self.job_id = job_id
        self.dag_jobid = dag_jobid

        try:
            self.dag_clusterid = int(self.dag_jobid.split(".")[0])
        except ValueError:
            pass

        if used_job_ad:
            # We can determine walltime and max memory from job ad.
            self.ad = job_ad
            if 'MaxWallTimeMinsRun' in self.ad:
                self.MAX_WALLTIME = int(self.ad['MaxWallTimeMinsRun']) * 60
            else:
                msg = "Unable to get MaxWallTimeMinsRun from job classads. Using the default MAX_WALLTIME."
                self.logger.debug(msg)
            if 'RequestMemory' in self.ad:
                self.MAX_MEMORY = int(self.ad['RequestMemory'])
            else:
                msg = "Unable to get RequestMemory from job classads. Using the default MAX_MEMORY."
                self.logger.debug(msg)
            msg = "Job ads already present. Will not use condor_q, but will load previous jobs ads."
            self.logger.debug(msg)
            self.get_job_ad_from_file()
        else:
            msg = "Will use condor_q command to get finished job ads from job_log."
            self.logger.debug(msg)
            self.get_job_ad_from_condor_q()

        # Do we still need identification of site in self.get_report()?
        # We can always get it from job ad.
        if 'JOBGLIDEIN_CMSSite' in self.ad:
            self.site = self.ad['JOBGLIDEIN_CMSSite']

        self.get_report()

        if self.ad.get("RemoveReason", "").startswith("Removed due to job being held"):
            hold_reason = self.ad.get("HoldReason", self.ad.get("LastHoldReason", "Unknown"))
            raise RecoverableError("Will retry held job; last hold reason: %s" % (hold_reason))

        try:
            self.check_empty_report()
            # Raises a RecoverableError or FatalError exception depending on the exitCode
            # saved in the job report.
            check_exit_code_retval = self.check_exit_code()
        except RecoverableError as e:
            orig_msg = str(e)
            try:
                self.check_memory_report()
                self.check_cpu_report()
                self.check_disk_report()
                self.check_expired_report()
            except Exception:  # pylint: disable=broad-except
                msg = "Original error: %s" % (orig_msg)
                self.logger.error(msg)
                raise
            raise

        # The fact that check_exit_code() has not raised RecoverableError or FatalError
        # doesn't necessarily mean that the job was successful; check_exit_code() reads
        # the job exit code from the job report and it might be that the exit code is
        # not there, in which case check_exit_code() returns silently. So as a safety
        # measure we check here the job return code passed to the post-job via the
        # DAGMan $RETURN argument macro. This is equal to the return code of the job
        # when the job was executed, and is equal to -100[1,2,3,4] when ["job failed to
        # be submitted to the batch system", "job externally removed from the batch
        # system queue", "error in the job log monitor", "job not executed because PRE
        # script returned non-zero"] respectively. In the particular case of job return
        # code = -1004 and job exit code from (previous job retry) job report = 0, we
        # should continue and not do the check. The reason is: the pre-job is not
        # expected to fail; if the pre-job returned non-zero we assume it was done so
        # in order to intentionally skip the job execution.
        if not (self.job_return_code == -1004 and check_exit_code_retval == 0):
            if self.job_return_code != JOB_RETURN_CODES.OK:  # Probably means stageout failed!
                msg = "Payload job was successful, but wrapper exited with non-zero status %d" % (self.job_return_code)
                if self.job_return_code > 0:
                    msg += " (stageout failure)?"
                raise RecoverableError(msg)

        return JOB_RETURN_CODES.OK

    # = = = = = RetryJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def execute(self, *args, **kw):
        """
        Need a doc string here.
        """
        try:
            job_status = self.execute_internal(*args, **kw)
            self.record_site(job_status)
            return job_status
        except RecoverableError as e:
            self.logger.error(str(e))
            self.record_site(JOB_RETURN_CODES.RECOVERABLE_ERROR)
            return JOB_RETURN_CODES.RECOVERABLE_ERROR
        except FatalError as e:
            self.logger.error(str(e))
            self.record_site(JOB_RETURN_CODES.FATAL_ERROR)
            return JOB_RETURN_CODES.FATAL_ERROR
        except Exception as e:  # pylint: disable=broad-except
            self.logger.exception(str(e))
            return 0  # Why do we return 0 here ?
