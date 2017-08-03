#! /usr/bin/env python

from __future__ import print_function

import os
import shutil
import tempfile
import unittest

from ServerUtilities import getTestDataDirectory

from TaskWorker.Actions.PreDAG import PreDAG


MISSING_LUMI_EXAMPLE = '{"1": [[41068, 41068], [41663, 41663], [42366, 42366], [42594, 42594], [43892, 43892], [44183, 44183], [44593, 44593], [45596, 45596], [45598, 45599], [45601, 45601], [46023, 46023], [46197, 46197], [46460, 46460], [47199, 47199], [47203, 47203], [47282, 47282], [50211, 50211], [50715, 50715], [50745, 50745], [50749, 50749], [50812, 50812], [51808, 51809], [51812, 51812], [52283, 52285], [52287, 52288]]}'


class PreDAGTest(unittest.TestCase):
    """
    _PreDAGTest_

    """

    def setUp(self):
        self.probes = ["0-{0}".format(i) for i in xrange(1, 6)]
        self.done = [str(i) for i in xrange(1, 7)]
        #we will work in a temporary directory and copy/create things there
        os.environ["TEST_DONT_REDIRECT_STDOUT"] = "True"
        self.olddir = os.getcwd()
        self.tempdir = tempfile.mkdtemp()
        os.chdir(self.tempdir)
        print("Working in {0}".format(self.tempdir))

        #status_cache file created by the task process
        os.mkdir("task_process")
        with(open("task_process/status_cache.txt", "w")) as fd:
            fd.write("0\n0\n{{{0}}}".format(",".join("'{0}': {{'State': 'finished'}}".format(i) for i in self.probes)))

        #Pickle file created by the TaskWorker
        shutil.copy(os.path.join(getTestDataDirectory(), "Actions", "datadiscovery.pkl"), self.tempdir)
        shutil.copy(os.path.join(getTestDataDirectory(), "Actions", "taskinformation.pkl"), self.tempdir)
        shutil.copy(os.path.join(getTestDataDirectory(), "Actions", "taskworkerconfig.pkl"), self.tempdir)

        #create throughtput files
        self.throughputdir = "automatic_splitting/throughputs/"
        os.makedirs(self.throughputdir)
        self.missing_lumidir = "automatic_splitting/missing_lumis/"
        os.makedirs(self.missing_lumidir)
        for p in self.probes + self.done:
            with(open(os.path.join(self.throughputdir, p), 'w')) as fd:
                fd.write("25.0")
        for p in self.done[:1]:
            with(open(os.path.join(self.missing_lumidir, p), 'w')) as fd:
                fd.write(MISSING_LUMI_EXAMPLE)

        #Touch the sandbox and TW code
        open("CMSRunAnalysis.tar.gz", "w").close()
        open("TaskManagerRun.tar.gz", "w").close()
        open("CMSRunAnalysis.sh", "w").close()


    def teardown(self):
        #back to the old dir and delete the temporary one
        os.chdir(self.olddir)
        shutil.rmtree(self.tempdir)

    def testSplitting(self):
        predag = PreDAG()
        predag.createSubdagSubmission = lambda *args: True
        self.assertEqual(predag.execute("processing", 5, 0), 0)

    def testTailSplitting(self):
        predag = PreDAG()
        predag.createSubdagSubmission = lambda *args: True
        predag.completedJobs = lambda *args: self.done
        predag.failedJobs = []
        self.assertEqual(predag.execute("tail", 6, 1), 0)
        with open('RunJobs1.subdag') as fd:
            self.assertEqual(sum(1 for _ in (line for line in fd if line.startswith('JOB'))), 1)

    def testProcessingJobsWithNoRetry(self):
        self.testSplitting()
        self.testTailSplitting()
        with open('RunJobs0.subdag') as fd:
            self.assertTrue(all([line.split()[2] == '0' for line in fd if line.startswith('RETRY')]))
        with open('RunJobs1.subdag') as fd:
            self.assertTrue(all([line.split()[2] != '0' for line in fd if line.startswith('RETRY')]))

if __name__ == '__main__':
    unittest.main()
