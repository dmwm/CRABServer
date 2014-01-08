
"""
Submit a DAG directory created by the DagmanCreator component.
"""

import os
import random
import traceback

import TaskWorker.Actions.TaskAction as TaskAction

from CRABInterface.Dagman.DagmanCreator import CRAB_HEADERS
from CRABInterface.Dagman.DagmanSnippets import *
from CRABInterface.Dagman.Fork import ReadFork
# Bootstrap either the native module or the BossAir variant.
try:
    import classad
    import htcondor
except ImportError:
    #pylint: disable=C0103
    classad = None
    htcondor = None
try:
    import WMCore.BossAir.Plugins.RemoteCondorPlugin as RemoteCondorPlugin
except ImportError:
    if not htcondor:
        raise ImportError, "You must have either the RemoteCondorPlugin or htcondor modules"
class DagmanSubmitter(TaskAction.TaskAction):

    """
    Submit a DAG to a HTCondor schedd
    """
    def __init__(self, *kargs, **kwargs):
        # python's inheritance can be a bitch sometimes
        self.forceSleepjobs = False
        TaskAction.TaskAction.__init__(self, *kargs, **kwargs)

    def getRemoteCondor(self):
        if self.config and hasattr(self.config, 'General') and hasattr(self.config.General, 'enableGsissh'):
            return self.config.General.enableGsissh
        elif htcondor:
            return False
        return True

    def getSchedd(self):
        """
        Determine a schedd to use for this task.
        """
        if self.getRemoteCondor():
            return self.config.BossAir.remoteUserHost
        collector = None
        if self.config and hasattr(self.config, 'General') and hasattr(self.config.General, 'condorPool'):
            collector = self.config.General.condorPool
        elif self.config and hasattr(self.config, 'TaskWorker') and hasattr(self.config.TaskWorker, 'htcondorPool'):
            collector = self.config.TaskWorker.htcondorPool
        schedd = "localhost"
        if self.config and hasattr(self.config, 'General') and hasattr(self.config.General, 'condorScheddList'):
            random.shuffle(self.config.General.condorScheddList)
            schedd = self.config.General.condorScheddList[0]
        elif self.config and hasattr(self.config, 'TaskWorker') and hasattr(self.config.TaskWorker, 'htcondorSchedds'):
            random.shuffle(self.config.TaskWorker.htcondorSchedds)
            schedd = self.config.TaskWorker.htcondorSchedds[0]
        if collector:
            return "%s:%s" % (schedd, collector)
        return schedd

    def getScheddObj(self, name):
        """
        Return a tuple (schedd, address) containing an object representing the
        remote schedd and its corresponding address.

        If address is None, then we are using the BossAir plugin.  Otherwise,
        the schedd object is of type htcondor.Schedd.
        """
        if not self.getRemoteCondor():
            if name == "localhost":
                schedd = htcondor.Schedd()
                with open(htcondor.param['SCHEDD_ADDRESS_FILE']) as fd:
                    address = fd.read().split("\n")[0]
            else:
                info = name.split(":")
                pool = "localhost"
                if len(info) == 2:
                    pool = info[1]
                coll = htcondor.Collector(self.getCollector(pool))
                scheddAd = coll.locate(htcondor.DaemonTypes.Schedd, info[0])
                address = scheddAd['MyAddress']
                schedd = htcondor.Schedd(scheddAd)
            return schedd, address
        else:
            return RemoteCondorPlugin.RemoteCondorPlugin(self.config, logger=self.logger), None

    def getCollector(self, name="localhost"):
        """
        Return an object representing the collector given the pool name.

        If the BossAir plugin is used, this simply returns the name
        """
        # Direct submission style
        if self.config and hasattr(self.config, 'General') and hasattr(self.config.General, 'condorPool'):
            return self.config.General.condorPool
        # TW style
        elif self.config and hasattr(self.config, 'TaskWorker') and hasattr(self.config.TaskWorker, 'htcondorPool'):
            return self.config.TaskWorker.htcondorPool
        return name

    def execute(self, *args, **kw):
        """
        dead code?
        """
        raise RuntimeError, "This code might be dead?"
        task = kw['task']
        print "ARGS %s" % args
        info = args[0][1]

        cwd = os.getcwd()
        os.chdir(tempDir)

        inputFiles = ['gWMS-CMSRunAnalysis.sh', task['tm_transformation'], 'cmscp.py', 'RunJobs.dag']
        inputFiles += [i for i in os.listdir('.') if i.startswith('Job.submit')]
        info['inputFilesString'] = ", ".join(inputFiles)
        outputFiles = ["RunJobs.dag.dagman.out", "RunJobs.dag.rescue.001"]
        info['outputFilesString'] = ", ".join(outputFiles)
        arg = "RunJobs.dag"

        try:
            info['remote_condor_setup'] = ''
            scheddName = sexit
            elf.getSchedd()
            schedd, address = self.getScheddObj(scheddName)
            if address:
                self.submitDirect(schedd, 'dag_bootstrap_startup.sh', arg, info)
            else:
                jdl = MASTER_DAG_SUBMIT_FILE % info
                schedd.submitRaw(task['tm_taskname'], jdl, task['userproxy'], inputFiles)
        finally:
            os.chdir(cwd)

    def submitRootJobFromDict(self, schedd, cmd, arg, info):
        ad = self.makeClassAd(self, info, cmd, arg)
        return self.submit.ClassAd(schedd)
    
    def submitClassAd(self, schedd, cmd, arg, ad, userproxy = None): #pylint: disable=R0201
        """
        Handles inserting a classad into htcondor
        """
        print ad
        with ReadFork() as (pid, rpipe, wpipe):
            if pid == 0:
                resultAds = []
                htcondor.SecMan().invalidateAllSessions()
                if userproxy:
                    os.environ['X509_USER_PROXY'] = userproxy
                htcondor.param['DELEGATE_FULL_JOB_GSI_CREDENTIALS'] = 'true'
                if ad.get('CRAB_SubmitScratchDir', None):
                    os.chdir(str(ad['CRAB_SubmitScratchDir']))
                schedd.submit(ad, 1, False, resultAds)
                #schedd.spool(resultAds)
                wpipe.write("OK")
        results = rpipe.read()
        if results != "OK":
            raise Exception("Failure when submitting HTCondor task: %s" % results)

        schedd.reschedule()
    
    def forceSleepJobs(self):
        """
            forceSleepJobs - For testing, make it so the payload is just a
                             really really long sleep and not the actual job
        """
        self.forceSleepJobs = True
