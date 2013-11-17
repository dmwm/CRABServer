
import json
import commands
import traceback

import classad

OK = 0
FATAL_ERROR = 2
RECOVERABLE_ERROR = 1

# Fatal error limits for job resource usage
MAX_WALLTIME = 21*60*60 + 30*60
MAX_MEMORY = 2*1024

class FatalError(Exception):
    pass

class RecoverableError(Exception):
    pass

class RetryJob(object):

    def __init__(self):
        self.count = "-1"

    def get_job_ad(self):
        cmd = "condor_q -l -userlog job_log %s" % str(self.cluster)
        status, output = commands.getstatusoutput(cmd)
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

    def get_report(self):
        try:
            with open('jobReport.json.%s' % self.count, 'r') as fd:
                try:
                    self.report = json.load(fd)
                except ValueError, ve:
                    self.report = {}
        except IOError, ioe:
            raise FatalError("IOError when reading the framework job report JSON: '%s'" % str(ioe))

    def check_cpu_report(self):
        if 'steps' not in self.report: return
        if 'cmsRun' not in self.report['steps']: return
        if 'performance' not in self.report['steps']['cmsRun']['performance']: return
        if 'cpu' not in self.report['steps']['cmsRun']['performance']: return
        if 'TotalJobTime' not in self.report['steps']['cmsRun']['performance']['cpu']: return

        totJobTime = self.report['steps']['cmsRun']['performance']['cpu']['TotalJobTime']
        try:
            totJobTime = float(totJobTime)
        except ValueError:
            return

        integratedJobTime = 0
        for ad in self.ads:
            if 'RemoteWallClockTime' in ad:
                integratedJobTime += ad['RemoteWallClockTime']

        # TODO: Compare the job against its requested walltime, not a hardcoded max.
        if totJobTime > MAX_WALLTIME:
            raise FatalError("Not retrying a long running job (job ran for %d hours)" % (totJobTime / 3600))
        if integratedJobTime > 1.5*MAX_WALLTIME:
            raise FatalError("Not retrying a job because the integrated time (across all retries) is %d hours." % (integratedJobTime / 3600))

    def check_memory_report(self):
        if 'steps' not in self.report: return
        if 'cmsRun' not in self.report['steps']: return
        if 'performance' not in self.report['steps']['cmsRun']['performance']: return
        if 'memory' not in self.report['steps']['cmsRun']['performance']: return
        if 'PeakValueRss' not in self.report['steps']['cmsRun']['performance']['memory']: return

        totJobMemory = self.report['steps']['cmsRun']['performance']['memory']['PeakValueRss']
        try:
            totJobMemory = float(totJobMemory)
        except ValueError:
            return

        # TODO: Compare the job against its requested memory, not a hardcoded max.
        if totJobMemory > MAX_MEMORY:
            raise FatalError("Not retrying job due to excessive memory use (%d MB)" % totJobMemory)

    def check_exit_code(self):
        if 'exitCode' not in self.report: return
        exitMsg = self.report.get("exitMsg", "UNKNOWN")

        try:
            exitCode = int(self.report['exitCode'])
        except ValueError:
            return

        # Wrapper script sometimes returns the posix return code (8 bits).
        if exitCode == 8021 or exitCode == 8028 or exitCode == 8020 or exitCode == 60307\
           exitCode == (8021%256) or exitCode == (8028%256) or exitCode == (8020%256) or exitCode == (60307%256):
            raise RecoverableError("Job failed to open local and fallback files.")
        if exitCode == 50513:
            raise RecoverableError("Job did not find functioning CMSSW on worker node.")
        # This is a difficult one -- right now CMSRunAnalysis.py will turn things like
        # segfaults into an invalid FJR.  Will revisit this decision later.
        if exitCode == 50115:
            raise RecoverableError("Job did not produce a FJR; will retry.")

        if exitCode:
            raise FatalError("Job exited with code %d.  Exit message: %s" % (exitCode, exitMsg))

    def check_empty_report(self):
        if not self.report:
            raise RecoverableError("Job did not produce a usable framework job report.")

    def execute_internal(self, status, retry_count, max_retries, count, cluster):

        self.count = count
        self.cluster = cluster
        self.get_job_ad()
        self.get_report()

        self.check_memory_report()
        self.check_cpu_report()

        self.check_empty_report()
        self.check_exit_code()

        if status: # Probably means stageout failed!
            raise RecoverableError("Payload job was successful, but exited wrapper exited non-zero status %d (stageout failure)?" % status)

        return OK

    def execute(self, *args, **kw):
        try:
            return self.execute_internal(*args, **kw)
        except FatalError, fe:
            print fe
            return FATAL_ERROR
        except RecoverableError, re:
            print re
            return RECOVERABLE_ERROR
        except Exception:
            print str(traceback.format_exc())
            return 0

