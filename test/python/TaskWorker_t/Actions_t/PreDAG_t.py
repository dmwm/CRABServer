#! /usr/bin/env python
""" Test module for the PreDAG class
"""
from __future__ import division
from __future__ import print_function

import os
import json
import shutil
import tempfile
import unittest

from ServerUtilities import getTestDataDirectory

from TaskWorker.Actions.PreDAG import PreDAG


MISSING_LUMI_EXAMPLE = ('{"1": [[41068, 41068], [41663, 41663], [42366, 42366], [42594, 42594], [43892, 43892], [44183, 44183], [44593, 44593], '
                        '[45596, 45596], [45598, 45599], [45601, 45601], [46023, 46023], [46197, 46197], [46460, 46460], [47199, 47199], [47203, '
                        '47203], [47282, 47282], [50211, 50211], [50715, 50715], [50745, 50745], [50749, 50749], [50812, 50812], [51808, 51809], '
                        '[51812, 51812], [52283, 52285], [52287, 52288]]}')


class PreDAGTest(unittest.TestCase):
    """ _PreDAGTest_

    """

    def createStatusCache(self, probe_status='finished', job_status='finished', overrides=None):
        """ Create a fake status cache.

        Use global status settings for all probe and processing jobs, with
        the option to override single job status settings by passing a
        dictionary as `overrides` parameter with job ids as keys and states
        as values.
        """
        if overrides is None:
            overrides = {}
        if not os.path.exists("task_process"):
            os.makedirs("task_process")
        data = {}
        for p in self.probes:
            data[p] = {'State': overrides.get(p, probe_status)}
        for j in self.done:
            data[j] = {'State': overrides.get(j, job_status)}
        with open("task_process/status_cache.txt", "w") as fd:
            fd.write("\n\n" + repr(data))

    def createFakePostjobData(self, jobs=None, throughput=25, eventSize=0, missingLumis=MISSING_LUMI_EXAMPLE):
        throughputdir = "automatic_splitting/throughputs/"
        missing_lumidir = "automatic_splitting/missing_lumis/"

        for d in (throughputdir, missing_lumidir):
            if os.path.exists(d):
                shutil.rmtree(d)
            os.makedirs(d)

        if not jobs:
            jobs = self.probes + self.done

        if throughput is not None and eventSize is not None:
            for p in jobs:
                with(open(os.path.join(throughputdir, p), 'w')) as fd:
                    json.dump((throughput, eventSize), fd)
        if missingLumis:
            for p in self.done[:1]:
                with(open(os.path.join(missing_lumidir, p), 'w')) as fd:
                    fd.write(missingLumis)

    def setUp(self):
        """ Create the all the necessary files
        """
        self.probes = ["0-{0}".format(i) for i in xrange(1, 6)]
        self.done = [str(i) for i in xrange(1, 7)]
        # we will work in a temporary directory and copy/create things there
        os.environ["TEST_DONT_REDIRECT_STDOUT"] = "True"
        # Override the PATH to overload condor_submit_dag with a script
        # that returns true; Otherwise, the tests will fail.
        os.environ['PATH'] = ':'.join([os.path.join(getTestDataDirectory(), "mock_condor"), os.environ['PATH']])

        self.olddir = os.getcwd()
        self.tempdir = tempfile.mkdtemp()
        os.chdir(self.tempdir)
        print("Working in {0}".format(self.tempdir))

        # status_cache file created by the task process
        self.createStatusCache()

        # Pickle file created by the TaskWorker
        shutil.copy(os.path.join(getTestDataDirectory(), "Actions", "datadiscovery.pkl"), self.tempdir)
        shutil.copy(os.path.join(getTestDataDirectory(), "Actions", "taskinformation.pkl"), self.tempdir)
        shutil.copy(os.path.join(getTestDataDirectory(), "Actions", "taskworkerconfig.pkl"), self.tempdir)
        shutil.copy(os.path.join(getTestDataDirectory(), "Actions", "run_and_lumis.tar.gz"), self.tempdir)

        # create throughtput files
        self.createFakePostjobData()

        # Touch the sandbox and TW code
        open("CMSRunAnalysis.tar.gz", "w").close()
        open("TaskManagerRun.tar.gz", "w").close()
        open("CMSRunAnalysis.sh", "w").close()

    def teardown(self):
        """ Back to the old dir and delete the temporary one
        """
        os.chdir(self.olddir)
        shutil.rmtree(self.tempdir)

    def testSplitting(self):
        """ Test that the PreDAG works when probe jobs finishes
        """
        predag = PreDAG()
        # Mock the createSubdagSubmission function
        self.assertEqual(predag.execute("processing", 5, 0), 0)
        with open('RunJobs0.subdag') as fd:
            self.assertEqual(sum(1 for _ in (line for line in fd if line.startswith('JOB'))), 50)

    def testSplittingWithHighOutput(self):
        """ Test that the PreDAG works when probe jobs finishes, with more jobs created due to large output size.
        """
        self.createFakePostjobData(eventSize=100000)
        predag = PreDAG()
        # Mock the createSubdagSubmission function
        self.assertEqual(predag.execute("processing", 5, 0), 0)
        with open('RunJobs0.subdag') as fd:
            self.assertGreater(sum(1 for _ in (line for line in fd if line.startswith('JOB'))), 100)

    def testTailSplitting(self):
        """ Test that the PreDAG works when there are tail jobs to create
        """
        predag = PreDAG()
        self.createStatusCache()
        self.assertEqual(predag.execute("tail", 6, 1), 0)
        with open('RunJobs1.subdag') as fd:
            self.assertEqual(sum(1 for _ in (line for line in fd if line.startswith('JOB'))), 1)

    def testTailSplittingWithAllProcessed(self):
        """ Test that the PreDAG works when there are no tail jobs to create
        """
        self.createFakePostjobData(missingLumis=None)
        predag = PreDAG()
        self.createStatusCache()
        self.assertEqual(predag.execute("tail", 6, 1), 0)
        self.assertEqual(os.path.exists('RunJobs1.subdag'), False)

    def testTailSplittingWithFailedProbesAndAllProcessed(self):
        """ Test that the PreDAG works when there are no tail jobs to create, but failed probes
        """
        self.createFakePostjobData(missingLumis=None)
        predag = PreDAG()
        self.createStatusCache(overrides=dict((p, 'failed') for p in self.probes[1:]))
        self.assertEqual(predag.execute("tail", 6, 1), 0)
        self.assertEqual(os.path.exists('RunJobs1.subdag'), False)

    def testTailSplittingWithFailedProbes(self):
        """ Test that the PreDAG works when there are tail jobs to create, and failed probes
        """
        predag = PreDAG()
        self.createStatusCache(overrides=dict((p, 'failed') for p in self.probes[1:]))
        self.assertEqual(predag.execute("tail", 6, 1), 0)
        with open('RunJobs1.subdag') as fd:
            self.assertEqual(sum(1 for _ in (line for line in fd if line.startswith('JOB'))), 1)

    def testProcessingJobsWithNoRetry(self):
        """ Check that the number of retries for processng jobs is changed to 0
        """
        self.testSplitting()
        self.testTailSplitting()
        with open('RunJobs0.subdag') as fd:
            self.assertTrue(all([line.split()[2] == '0' for line in fd if line.startswith('RETRY')]))
        with open('RunJobs1.subdag') as fd:
            self.assertTrue(all([line.split()[2] != '0' for line in fd if line.startswith('RETRY')]))

    def testAllProcessingFailed(self):
        """ Check that we don't fail if all processing jobs fail
        """
        self.createFakePostjobData(jobs=self.probes, missingLumis=None)
        self.createStatusCache(job_status='failed')
        predag = PreDAG()
        predag.createSubdag = lambda *args: True
        self.assertEqual(predag.execute("tail", 6, 1), 0)
        with open('RunJobs1.subdag') as fd:
            self.assertEqual(sum(1 for _ in (line for line in fd if line.startswith('JOB'))), 17)


if __name__ == '__main__':
    unittest.main()
