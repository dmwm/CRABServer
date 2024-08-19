""" Resubmit failed jobs in tasks """

import os

from http.client import HTTPException
from urllib.parse import urlencode

import HTCondorLocator

from ServerUtilities import FEEDBACKMAIL
from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.Actions.DagmanSubmitter import checkMemoryWalltime
from TaskWorker.WorkerExceptions import TaskWorkerException
from TaskWorker.DataObjects import  Result

if 'useHtcV2' in os.environ:
    import htcondor2 as htcondor
    import classad2 as classad
else:
    import htcondor
    import classad


class DagmanResubmitter(TaskAction):
    """
    Given a task name, resubmit failed tasks.

    Internally, we simply release the failed DAG.
    """

    def executeInternal(self, *args, **kwargs): #pylint: disable=unused-argument
        """ real work happens here """
        #Marco: I guess these value errors only happens for development instances
        if 'task' not in kwargs:
            raise ValueError("No task specified.")
        task = kwargs['task']
        if 'tm_taskname' not in task:
            raise ValueError("No taskname specified.")
        workflow = str(task['tm_taskname'])
        if 'user_proxy' not in task:
            raise ValueError("No proxy provided")

        self.logger.info("About to resubmit failed jobs for workflow: %s.", workflow)
        self.logger.debug("Task info: %s", str(task))

        if task['tm_collector']:
            self.backendurls['htcondorPool'] = task['tm_collector']
        loc = HTCondorLocator.HTCondorLocator(self.backendurls)

        schedd = ""
        dummyAddress = ""
        schedName = task['tm_schedd'].split('@')[-1]  # works also if schedname does not have @
        try:
            schedd, dummyAddress = loc.getScheddObjNew(task['tm_schedd'])
        except Exception as exp:
            msg  = f"The CRAB TaskWorker was not able to contact Grid scheduler: {schedName}."
            msg += " Please try again later."
            msg += f" If the error persists send an e-mail to {FEEDBACKMAIL}."
            msg += f" Message from the scheduler:\n{exp}"
            self.logger.exception("%s: %s", workflow, msg)
            raise TaskWorkerException(msg) from exp

        # Check memory and walltime
        checkMemoryWalltime(None, task, 'resubmit', self.logger, self.uploadWarning)


        # Find only the originally submitted DAG to hold and release: this
        # will re-trigger the scripts and adjust retries and other
        # resubmission parameters.
        #
        # Processing and tail DAGs will be restarted by these scrips on the
        # schedd after the modifications are made.
        rootConst = f"(CRAB_DAGType =?= \"BASE\" && CRAB_ReqName =?= {classad.quote(workflow)})"
        rootConst += f" || (TaskType =?= \"ROOT\" && CRAB_ReqName =?= {classad.quote(workflow)})"

        ## Calculate new parameters for resubmitted jobs. These parameters will
        ## be (re)written in the _CONDOR_JOB_AD when we do schedd.edit() below.
        ad = classad.ClassAd()
        params = {'CRAB_ResubmitList'  : 'jobids',
                  'CRAB_SiteBlacklist' : 'site_blacklist',
                  'CRAB_SiteWhitelist' : 'site_whitelist',
                  'MaxWallTimeMinsRun' : 'maxjobruntime',
                  'RequestMemory'      : 'maxmemory',
                  'RequestCpus'        : 'numcores',
                  'JobPrio'            : 'priority'
                 }
        for taskparam in params.values():
            if ('resubmit_'+taskparam in task) and task['resubmit_'+taskparam] is not None:
                # the following seems to Stefano a very complicated way to handle
                # in different (proper) way the cases where resubmit_xxxx is list and
                # where it is a number. Should be verified and simplified
                if isinstance(task['resubmit_'+taskparam], list):
                    ad[taskparam] = task['resubmit_'+taskparam]
        if ('resubmit_jobids' in task) and task['resubmit_jobids']:
            self.logger.debug("Resubmitting when JOBIDs were specified")
            try:
                schedd.edit(rootConst, "HoldKillSig", 'SIGKILL')
                # Overwrite parameters in the os.environ[_CONDOR_JOB_AD] file. This will affect
                # all the jobs, not only the ones we want to resubmit. That's why the pre-job
                # is saving the values of the parameters for each job retry in text files (the
                # files are in the directory resubmit_info in the schedd).
                for adparam, taskparam in params.items():
                    if taskparam in ad:
                        # repr() in the line below is a workaround for V2 bindings bug
                        # https://github.com/dmwm/CRABServer/issues/8604#issuecomment-2284346056
                        schedd.edit(rootConst, adparam, repr(ad.lookup(taskparam)))
                    elif task['resubmit_' + taskparam] is not None:
                        schedd.edit(rootConst, adparam, str(task['resubmit_' + taskparam]))
                schedd.act(htcondor.JobAction.Hold, rootConst)
                schedd.edit(rootConst, "HoldKillSig", 'SIGUSR1')
                schedd.act(htcondor.JobAction.Release, rootConst)
            except Exception as hte:
                msg = "The CRAB server backend was not able to resubmit the task,"
                msg += " because the Grid scheduler answered with an error."
                msg += " This is probably a temporary glitch. Please try again later."
                msg += f" If the error persists send an e-mail to {FEEDBACKMAIL}."
                msg += f" Error reason: {hte}"
                raise TaskWorkerException(msg) from hte
        else:
            # current (Apr 2024) resubmit command in CRABClient always sends a list of jobIds
            # so we never reach here. Keep it for future submission reworking along
            # https://github.com/dmwm/CRABServer/issues/6270
            self.logger.debug("Resubmitting when JOBIDs were NOT specified")
            raise NotImplementedError



    def execute(self, *args, **kwargs):
        """
        The execute method of the DagmanResubmitter class.
        """
        self.executeInternal(*args, **kwargs)
        try:
            configreq = {'subresource': 'state',
                         'workflow': kwargs['task']['tm_taskname'],
                         'status': 'SUBMITTED'}
            self.logger.debug("Setting the task as successfully resubmitted with %s", str(configreq))
            self.crabserver.post(api='workflowdb', data=urlencode(configreq))
        except HTTPException as hte:
            self.logger.error(hte.headers)
            msg  = "The CRAB server successfully resubmitted the task to the Grid scheduler,"
            msg += f" but was unable to update the task status to {configreq['status']} in the database."
            msg += " This should be a harmless (temporary) error."
            raise TaskWorkerException(msg) from hte
        return Result.Result(task=kwargs['task'], result='OK')

if __name__ == "__main__":
    import logging
    from RESTInteractions import CRABRest
    from WMCore.Configuration import Configuration

    logging.basicConfig(level=logging.DEBUG)
    config = Configuration()

    config.section_("TaskWorker")
    #will use X509_USER_PROXY var for this test
    config.TaskWorker.cmscert = os.environ["X509_USER_PROXY"]
    config.TaskWorker.cmskey = os.environ["X509_USER_PROXY"]

    server_ = CRABRest('cmsweb-testbed.cern.ch', config.TaskWorker.cmscert, config.TaskWorker.cmskey)
    server_.setDbInstance('dev')
    resubmitter = DagmanResubmitter(config, server_)
    resubmitter.execute(task={'tm_taskname':'141129_110306_crab3test-5:atanasi_crab_test_resubmit', 'user_proxy' : os.environ["X509_USER_PROXY"],
                              'resubmit_site_whitelist' : ['T2_IT_Bari'], 'resubmit_site_blacklist' : ['T2_IT_Legnaro'], 'resubmit_priority' : 2,
                              'resubmit_numcores' : 1, 'resubmit_maxjobruntime' : 1000, 'resubmit_maxmemory' : 1000
                             })
