#! /usr/bin/env python

from __future__ import print_function

import os
import shutil
import tempfile
import unittest

from ServerUtilities import getTestDataDirectory

from TaskWorker.Actions.PreDAG import PreDAG


class PreDAGTest(unittest.TestCase):
    """
    _PreDAGTest_

    """

    def setUp(self):
        probes = list(xrange(1, 6))
        #we will work in a temporary directory and copy/create things there
        os.environ["TEST_DONT_REDIRECT_STDOUT"] = "True"
        self.olddir = os.getcwd()
        self.tempdir = tempfile.mkdtemp()
        os.chdir(self.tempdir)
        print("Working in {0}".format(self.tempdir))

        #status_cache file created by the task process
        os.mkdir("task_process")
        with(open("task_process/status_cache.txt", "w")) as fd:
            fd.write("0\n0\n{{{0}}}".format(",".join("'0-{0}': {{'State': 'finished'}}".format(i) for i in probes)))

        #Pickle file created by the TaskWorker
        shutil.copy(os.path.join(getTestDataDirectory(), "Actions", "datadiscovery.pkl"), self.tempdir)
        shutil.copy(os.path.join(getTestDataDirectory(), "Actions", "taskinformation.pkl"), self.tempdir)
        shutil.copy(os.path.join(getTestDataDirectory(), "Actions", "taskworkerconfig.pkl"), self.tempdir)

        #create throughtput files
        self.throughputdir = "automatic_splitting/throughputs/"
        os.makedirs(self.throughputdir)
        for p in probes:
            with(open(os.path.join(self.throughputdir, "0-{0}".format(p)), 'w')) as fd:
                fd.write("25.0")

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
        predag.createSubdagSubmission = lambda x, y, z: True
        self.assertEqual(predag.execute("processing", 5, 0), 0)

    def testTailSplitting(self):
        pass

if __name__ == '__main__':
    unittest.main()
