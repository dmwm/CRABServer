
import urllib
import traceback

import htcondor

import HTCondorLocator
import HTCondorUtils

import TaskWorker.Actions.TaskAction as TaskAction

class DagmanResubmitter(TaskAction.TaskAction):

    """
    Given a task name, resubmit failed tasks.

    Internally, we simply release the failed DAG.
    """

    def execute(self, *args, **kw):

        if 'task' not in kw:
            raise ValueError("No task specified.")
        task = kw['task']
        if 'tm_taskname' not in task:
            raise ValueError("No taskname specified.")
        workflow = str(task['tm_taskname'])
        if 'user_proxy' not in task:
            raise ValueError("No proxy provided")
        proxy = task['user_proxy']

        self.logger.info("About to kill workflow: %s. Getting status first." % workflow)

        loc = HTCondorLocator.HTCondorLocator(self.config)
        scheddName = loc.getSchedd()
        schedd, address = loc.getScheddObj(scheddName)

        # Release the DAG
        rootConst = "TaskType =?= \"ROOT\" && CRAB_ReqName =?= %s" % HTCondorUtils.quote(workflow)

        with HTCondorUtils.AuthenticatedSubprocess(proxy) as (parent, rpipe):
            if not parent:
                schedd.act(htcondor.JobAction.Release, rootConst)
        results = rpipe.read()
        if results != "OK":
            raise Exception("Failure when killing job: %s" % results)

