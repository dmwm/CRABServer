"""Usage: PreDAG.py stage completion prefix

This is the PreDAG script that is executed after the probe jobs finishes and
after the processing jobs finishes.

It accepts three parameters: stage (which can either be processing or tail),
completion that determines when the script starts doing things, and prefix that
in case of completion jobs is prefixed to the subjobs numbers (1-1, 1-2, 1-3 for
completion job number 1).

In processing mode it looks for Job0-\\d+ (probe jobs) and when N=completion jobs have finished
it starts and it does the "regular splitting" creating the processing jobs.

In tail mode it looks for Job[1-9]\\d+$ (the processing jobs), and again
it starts when N=Completion jobs have finished. Again, the splitting is
performed but using as lumi mask the unprocessed lumis (lumis missing from
jobs that did not complete in time). It also includes failed jobs.
"""
from __future__ import division
from __future__ import print_function

import os
import re
import sys
import json
import errno
import pickle
import shutil
import logging
import tarfile
import tempfile
import subprocess

from ast import literal_eval
from WMCore.DataStructs.LumiList import LumiList

from ServerUtilities import getLock
from TaskWorker.Actions.Splitter import Splitter
from TaskWorker.Actions.DagmanCreator import DagmanCreator
from TaskWorker.WorkerExceptions import TaskWorkerException

#TODO In general better logging and documentation

class PreDAG:
    def __init__(self):
        """PreDAG constructor"""
        self.stage = None
        self.completion = None
        self.prefix = None
        self.statusCacheInfo = None
        self.processedJobs = None
        self.failedJobs = None
        self.logger = logging.getLogger()
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", \
                                      datefmt = "%a, %d %b %Y %H:%M:%S %Z(%z)")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False

    def readJobStatus(self):
        """Read the job status(es) from the cache_status file and save the relevant info into self.statusCacheInfo"""
        #XXX Maybe the status_cache filname should be in a variable in ServerUtilities?
        if not os.path.exists("task_process/status_cache.txt"):
            return
        with open("task_process/status_cache.txt") as fd:
            fileContent = fd.read()
            #TODO Splitting '\n' and accessing the second element is really fragile.
            #It is what it is done in the client though, but we should change it
            self.statusCacheInfo = literal_eval(fileContent.split('\n')[2])

    def readProcessedJobs(self):
        """Read processed job ids"""
        if not os.path.exists("automatic_splitting/processed"):
            self.processedJobs = set()
            return
        with open("automatic_splitting/processed", "rb") as fd:
            self.processedJobs = pickle.load(fd)

    def saveProcessedJobs(self, jobs):
        """Update processed job ids"""
        with open("automatic_splitting/processed", "wb") as fd:
            pickle.dump(self.processedJobs.union(jobs), fd)

    def completedJobs(self):
        """Yield job IDs of completed (finished or failed) jobs.  All
        failed jobs are saved in self.failedJobs, too.
        """
        self.failedJobs = []
        stagere = {}
        stagere['processing'] = re.compile(r"^0-\d+$")
        stagere['tail'] = re.compile(r"^[1-9]\d*$")
        completedCount = 0
        for jobnr, jobdict in self.statusCacheInfo.iteritems():
            state = jobdict.get('State')
            if stagere[self.stage].match(jobnr) and state in ('finished', 'failed'):
                if state == 'failed':
                    self.failedJobs.append(jobnr)
                completedCount += 1
                yield jobnr
        self.logger.info("found {0} completed jobs".format(completedCount))

    def execute(self, *args):
        """Excecute executeInternal in locked mode
        """
        self.logger.debug("Acquiring PreDAG lock")
        with getLock("PreDAG") as _lock:
            self.logger.debug("PreDAGlock acquired")
            retval = self.executeInternal(*args)
        self.logger.debug("PreDAG lock released")
        return retval

    def setupLog(self):
        """Create the predag.prefix.txt file and make sure stdout and stderr file handlers are
        duplicated and then redirected there
        """
        ## Create a directory in the schedd where to store the predag logs.
        logpath = os.path.join(os.getcwd(), "prejob_logs")
        try:
            os.makedirs(logpath)
        except OSError as ose:
            if ose.errno != errno.EEXIST:
                logpath = os.getcwd()
        ## Create (open) the pre-dag log file predag.<prefix>.txt.
        predagLogFileName = os.path.join(logpath, "predag.{0}.txt".format(self.prefix))
        fdPredagLog = os.open(predagLogFileName, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
        os.chmod(predagLogFileName, 0o644)
        ## Redirect stdout and stderr to the pre-dag log file.
        if os.environ.get('TEST_DONT_REDIRECT_STDOUT', False):
            print("Pre-DAG started with no output redirection.")
        else:
            os.dup2(fdPredagLog, 1)
            os.dup2(fdPredagLog, 2)
            msg = "Pre-DAG started with output redirected to %s" % (predagLogFileName)
            self.logger.info(msg)

    def executeInternal(self, *args):
        """The executeInternal method return 4 if the "completion" threshold is not reached, 0 otherwise"""
        self.stage = args[0]
        self.completion = int(args[1])
        self.prefix = args[2]

        self.setupLog()

        self.statusCacheInfo = {} #Will be filled with the status from the status cache

        self.readJobStatus()
        completed = set(self.completedJobs())
        if len(completed) < self.completion:
            return 4

        self.readProcessedJobs()
        unprocessed = completed - self.processedJobs
        self.logger.info("jobs remaining to process: {0}".format(", ".join(sorted(unprocessed))))

        # The TaskWorker saves some files that now we are gonna read
        with open('datadiscovery.pkl', 'rb') as fd:
            dataset = pickle.load(fd) #Output from the discovery process
        with open('taskinformation.pkl', 'rb') as fd:
            task = pickle.load(fd) #A dictionary containing information about the task as in the Oracle DB
        with open('taskworkerconfig.pkl', 'rb') as fd:
            config = pickle.load(fd) #Task worker configuration

        # Read the automatic_splitting/throughputs/0-N files where the PJ
        # saved the EventThroughput (report['steps']['cmsRun']['performance']['cpu']['EventThroughput'])
        sumEventsThr = 0
        count = 0
        for jid in unprocessed:
            if jid in self.failedJobs:
                continue
            fn = "automatic_splitting/throughputs/{0}".format(jid)
            with open(fn) as fd:
                sumEventsThr += float(fd.read())
                count += 1
        eventsThr = sumEventsThr / count
        self.logger.info("average throughput for {1} jobs: {0}".format(eventsThr, count))
        runtime = task['tm_split_args'].get('seconds_per_job', -1)
        if self.stage == "processing":
            # Build in a 33% error margin in the runtime to not create too
            # many tails. This essentially moves the peak to lower
            # runtimes and cuts off less of the job distribution tail.
            target = int(0.75 * runtime)
        elif self.stage == 'tail':
            target = int(max(
                getattr(config.TaskWorker, 'automaticTailRuntimeMinimum', 45 * 60),
                getattr(config.TaskWorker, 'automaticTailRuntimeFraction', 0.2) * runtime
            ))
        events = int(target * eventsThr)
        splitTask = dict(task)
        splitTask['tm_split_algo'] = 'EventAwareLumiBased'
        splitTask['tm_split_args']['events_per_job'] = events

        if self.stage == 'tail' and not self.adjustLumisForCompletion(splitTask, unprocessed):
            self.logger.info("nothing to process for completion")
            self.saveProcessedJobs(unprocessed)
            return 0

        try:
            config.TaskWorker.scratchDir = './scratchdir' # XXX
            splitter = Splitter(config, server=None, resturi='')
            split_result = splitter.execute(dataset, task=splitTask)
            self.logger.info("Splitting results:")
            for g in split_result.result[0]:
                msg = "Created jobgroup with length {0}".format(len(g.getJobs()))
                self.logger.info(msg)
        except TaskWorkerException as e:
            retmsg = "Splitting failed with:\n{0}".format(e)
            self.logger.error(msg)
#            self.set_dashboard_state('FAILED')
            return 1
        try:
            creator = DagmanCreator(config, server=None, resturi='')
            parent = self.prefix if self.stage == 'tail' else None
            _, _, subdags = creator.createSubdag(split_result.result, task=task, parent=parent, stage=self.stage)
            if self.stage == 'processing':
                self.createSubdagSubmission(['RunJobs0.subdag'], getattr(config.TaskWorker, 'maxPost', 20), 'processing')
            self.logger.info("creating following subdags: {0}".format(", ".join(subdags)))
            self.createSubdagSubmission(subdags, getattr(config.TaskWorker, 'maxPost', 20), 'tail')
        except TaskWorkerException as e:
            retmsg = "DAG creation failed with:\n{0}".format(e)
            self.logger.error(retmsg)
#            self.set_dashboard_state('FAILED')
            return 1
        self.saveProcessedJobs(unprocessed)
        return 0

    def createSubdagSubmission(self, subdags, maxpost, stage):
        #TODO Not tested, did not work
        for dag in subdags:
            subprocess.check_call(['condor_submit_dag', '-DoRecov', '-AutoRescue', '0', '-MaxPre', '20', '-MaxIdle', '1000',
                '-MaxPost', str(maxpost), '-no_submit', '-insert_sub_file', 'subdag.ad',
                '-append', '+Environment = strcat(Environment," _CONDOR_DAGMAN_LOG={0}/{1}.dagman.out")'.format(os.getcwd(), dag),
                '-append', '+TaskType = "{0}"'.format(stage.upper()), dag])

    def adjustLumisForCompletion(self, task, unprocessed):
        """Sets the run, lumi information in the task information for the
        completion jobs.  Returns True if completion jobs are needed,
        otherwise False.
        """
        missingDir = "automatic_splitting/missing_lumis/" #TODO in ServerUtilities to be shared with PJ

        try:
            available = set(os.listdir(missingDir)) & unprocessed
        except OSError:
            available = set()

        failed = set(self.failedJobs) & unprocessed

        if len(available) == 0 and len(failed) == 0:
            return False

        missing = LumiList()
        for missingFile in available:
            with open(os.path.join(missingDir, missingFile)) as fd:
                self.logger.info("Adding missing lumis from job %s", missingFile)
                missing = missing + LumiList(compactList=literal_eval(fd.read()))
        for failedId in failed:
            try:
                tmpdir = tempfile.mkdtemp()
                f = tarfile.open("run_and_lumis.tar.gz")
                fn = "job_lumis_{0}.json".format(failedId)
                f.extract(fn, path=tmpdir)
                with open(os.path.join(tmpdir, fn)) as fd:
                    injson = json.load(fd)
                    missing = missing + LumiList(compactList=injson)
                    self.logger.info("Adding lumis from failed job %s", failedId)
            finally:
                f.close()
                shutil.rmtree(tmpdir)
        missing_compact = missing.getCompactList()
        runs = missing.getRuns()
        #Compact list is like
        #{
        #'1': [[1, 33], [35, 35], [37, 47], [49, 75], [77, 130], [133, 136]],
        #'2':[[1,45],[50,80]]
        #}
        #Now we turn lumis it into something like:
        #lumis=['1, 33, 35, 35, 37, 47, 49, 75, 77, 130, 133, 136','1,45,50,80']
        #which is the format expected by buildLumiMask in the splitting algorithm
        lumis = [",".join(map(str, reduce(lambda x, y:x + y, missing_compact[run]))) for run in runs]

        task['tm_split_args']['runs'] = runs
        task['tm_split_args']['lumis'] = lumis

        return True


if __name__ == '__main__':
    sys.exit(PreDAG().execute(sys.argv[1:]))
