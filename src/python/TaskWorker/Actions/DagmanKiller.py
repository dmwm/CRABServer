
import re
import urllib
import traceback

import htcondor

import TaskWorker.Actions.TaskAction as TaskAction

import HTCondorLocator
import HTCondorUtils

WORKFLOW_RE = re.compile("[a-z0-9_]+")

class DagmanKiller(TaskAction.TaskAction):

    """
    Given a task name, kill the corresponding task in HTCondor.

    We do not actually "kill" the task off, but put the DAG on hold.
    """

    def executeInternal(self, *args, **kw):

        if 'task' not in kw:
            raise ValueError("No task specified.")
        task = kw['task']
        if 'tm_taskname' not in task:
            raise ValueError("No taskname specified")
        workflow = task['tm_taskname']
        if 'user_proxy' not in task:
            raise ValueError("No proxy provided")
        proxy = task['user_proxy']

        self.logger.info("About to kill workflow: %s. Getting status first." % workflow)

        workflow = str(workflow)
        if not WORKFLOW_RE.match(workflow):
            raise Exception("Invalid workflow name.")

        loc = HTCondorLocator.HTCondorLocator(self.config)
        scheddName = loc.getSchedd()
        schedd, address = loc.getScheddObj(scheddName)

        # Search for and hold the DAG
        rootConst = "TaskType =?= \"ROOT\" && CRAB_ReqName =?= %s" % HTCondorUtils.quote(workflow)
        rootAttrList = ["ClusterId"]

        with HTCondorUtils.AuthenticatedSubprocess(proxy) as (parent, rpipe):
            if not parent:
               schedd.act(htcondor.JobAction.Hold, rootConst)
        results = rpipe.read()
        if results != "OK":
            raise Exception("Failure when killing task: %s" % results)

    def execute(self, *args, **kw):

        try:
            self.executeInternal(*args, **kw)
        except Exception, exc:
            self.logger.error(str(traceback.format_exc()))
        finally:
            if kw['task']['kill_all']:
                configreq = {'workflow': kw['task']['tm_taskname'], 'status': "KILLED"}
                self.server.post(self.resturl, data = urllib.urlencode(configreq))
            else:
                # TODO: Not sure what this does.
                configreq = {'workflow': kw['task']['tm_taskname'], 'status': "SUBMITTED"}
                self.server.post(self.resturl, data = urllib.urlencode(configreq))

