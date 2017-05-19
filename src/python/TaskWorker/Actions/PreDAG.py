""" Usage: PreDAG.py stage completion prefix

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
    jobs that did not complete in tiem). Including failed jobs is under discussion.
"""
from __future__ import division
from __future__ import print_function

import os
import re
import sys
import glob
import errno
import pickle
import logging
import subprocess

from ast import literal_eval
from WMCore.Configuration import Configuration
from WMCore.Configuration import ConfigSection
from TaskWorker.Actions.Splitter import Splitter
from TaskWorker.Actions.DagmanCreator import DagmanCreator
from TaskWorker.WorkerExceptions import TaskWorkerException

#TODO In general better logging and documentation

class PreDAG:
    def __init__(self):
        """
        PreDAG constructor.
        """
        self.stage = None
        self.completion = None
        self.prefix = None
        self.logger = logging.getLogger()
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", \
                                      datefmt = "%a, %d %b %Y %H:%M:%S %Z(%z)")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False

    def readJobStatus(self):
        """ Read the job status(es) from the cache_status file
        """
        #XXX Maybe the status_cache filname should be in a variable in ServerUtilities?
        if not os.path.exists("task_process/status_cache.txt"):
            return
        with open("task_process/status_cache.txt") as fd:
            fileContent = fd.read()
            #TODO Splitting '\n' and accessing the second element is really fragile.
            #It is what it is done in the client though, but we should change it
            self.statusCacheInfo = literal_eval(fileContent.split('\n')[2])

    def hitCompletionThreshold(self):
        """ The method checks if N==completion jobs are in the 'finished' state
            If so it returns True, otherwise it returns False
        """
        stagere = {}
        stagere['processing'] = re.compile(r"^0-\d+$")
        stagere['completion'] = re.compile(r"^[1-9]\d+$")
        completedCount = 0
        for jobnr, jobdict in self.statusCacheInfo.iteritems():
            state = jobdict.get('State')
            if stagere[self.stage].match(jobnr) and state == 'finished':
                completedCount += 1
                if completedCount == self.completion:
                    return True
        self.logger.info("found {0} completed jobs".format(completedCount))
        return False

    def execute(self, *args):
        """ The execution method return 4 if the "completion" threshold is not reached, 0 otherwise
        """
        self.stage = args[0]
        self.completion = int(args[1])
        self.prefix = args[2]

        ## Create a directory in the schedd where to store the predag logs.
        logpath = os.path.join(os.getcwd(), "prejob_logs")
        try:
            os.makedirs(logpath)
        except OSError as ose:
            if ose.errno != errno.EEXIST:
                logpath = os.getcwd()
        ## Create (open) the pre-dag log file predag.<prefix>.txt.
        predag_log_file_name = os.path.join(logpath, "predag.{0}.txt".format(self.prefix))
        fd_predag_log = os.open(predag_log_file_name, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
        os.chmod(predag_log_file_name, 0o644)

        ## Redirect stdout and stderr to the pre-dag log file.
        if os.environ.get('TEST_DONT_REDIRECT_STDOUT', False):
            print("Pre-DAG started with no output redirection.")
        else:
            os.dup2(fd_predag_log, 1)
            os.dup2(fd_predag_log, 2)
            msg = "Pre-DAG started with output redirected to %s" % (predag_log_file_name)
            self.logger.info(msg)

        self.statusCacheInfo = {} #Will be filled with the status from the status cache

        self.readJobStatus()
        if not self.hitCompletionThreshold():
            return 4
        with open('datadiscovery.pkl', 'rb') as fd:
            dataset = pickle.load(fd)
        with open('taskinformation.pkl', 'rb') as fd:
            task = pickle.load(fd)
        with open('taskworkerconfig.pkl', 'rb') as fd:
            config = pickle.load(fd)
        maxpost = getattr(config.TaskWorker, 'maxPost', 20)

        #TODO refactor
        if self.stage == "processing":
            # Build in a 33% error margin in the runtime to not create too
            # many tails. This essentially moves the peak to lower
            # runtimes and cuts off less of the job distribution tail.
            target = int(0.75 * task['tm_split_args']['seconds_per_job'])
            # Read the automatic_splitting/throughputs/0-N files where the PJ
            # saved the EventThroughput (report['steps']['cmsRun']['performance']['cpu']['EventThroughput'])
            sumEventsThr = 0
            count = 0
            for fn in glob.glob("automatic_splitting/throughputs/0-*"):
                with open(fn) as fd:
                    sumEventsThr += float(fd.read())
                    count += 1
            eventsThr = sumEventsThr / count
            events = int(target * eventsThr)
            task['tm_split_algo'] = 'EventAwareLumiBased'
            task['tm_split_args']['events_per_job'] = events

            try:
                config.TaskWorker.scratchDir = './scratchdir' # XXX
                splitter = Splitter(config, server=None, resturi='')
                split_result = splitter.execute(dataset, task=task)
                self.logger.info("Splitting results:")
                for g in split_result.result[0]:
                    msg = "Created jobgroup with length {0}".format(len(g.getJobs()))
                    self.logger.info(msg)
            except TaskWorkerException as e:
                self.logger.error("Error during splitting:\n{0}".format(e))
                self.set_dashboard_state('FAILED')
                retmsg = "Splitting failed with:\n{0}".format(e)
                return 1
            try:
                creator = DagmanCreator(config, server=None, resturi='')
                parent = self.prefix if self.stage == 'tail' else None
                _, _, subdags = creator.createSubdag(split_result.result, task=task, parent=parent, stage='processing')
                self.logger.info("creating following subdags: {0}".format(", ".join(subdags)))
                self.createSubdagSubmission(subdags, getattr(config.TaskWorker, 'maxPost', 20))
            except TaskWorkerException as e:
                self.logger.error('Error during subdag creation\n{0}'.format(e))
                self.set_dashboard_state('FAILED')
                retmsg = "DAG creation failed with:\n{0}".format(e)
                return 1
        return 0

    def createSubdagSubmission(self, subdags, maxpost):
        #TODO Not tested, did not work
        for dag in subdags:
            subprocess.check_call(['condor_submit_dag', '-AutoRescue', '0', '-MaxPre', '20', '-MaxIdle', '1000',
                '-MaxPost', str(maxpost), '-no_submit', '-insert_sub_file', 'subdag.ad',
                '-append', '+Environment = strcat(Environment," _CONDOR_DAGMAN_LOG={0}/{1}.dagman.out")'.format(os.getcwd(), dag), dag])



if __name__ == '__main__':
    sys.exit(PreDAG().execute(sys.argv[1:]))
