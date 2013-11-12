
import re
import urllib
import traceback

import classad
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
        self.task = kw['task']
        if 'tm_taskname' not in self.task:
            raise ValueError("No taskname specified")
        self.workflow = self.task['tm_taskname']
        if 'user_proxy' not in self.task:
            raise ValueError("No proxy provided")
        self.proxy = self.task['user_proxy']

        self.logger.info("About to kill workflow: %s. Getting status first." % self.workflow)

        self.workflow = str(self.workflow)
        if not WORKFLOW_RE.match(self.workflow):
            raise Exception("Invalid workflow name.")

        loc = HTCondorLocator.HTCondorLocator(self.config)
        scheddName = loc.getSchedd()
        self.schedd, address = loc.getScheddObj(scheddName)

        if self.task['kill_all']:
            return self.killAll()
        else:
            return self.killJobs(self.task['kill_ids'])

    def killJobs(self, ids):
        ad = classad.ClassAd()
        ad['foo'] = ids
        const = "CRAB_ReqName =?= %s && member(CRAB_Id, %s)" % (HTCondorUtils.quote(self.workflow), ad.lookup("foo").__repr__())
        with HTCondorUtils.AuthenticatedSubprocess(self.proxy) as (parent, rpipe):
            if not parent:
               self.schedd.act(htcondor.JobAction.Remove, const)
        results = rpipe.read()
        if results != "OK":
            raise Exception("Failure when killing jobs [%s]: %s" % (", ".join(ids), results))

    def killAll(self):

        # Search for and hold the DAG
        rootConst = "TaskType =?= \"ROOT\" && CRAB_ReqName =?= %s" % HTCondorUtils.quote(self.workflow)
        rootAttrList = ["ClusterId"]

        with HTCondorUtils.AuthenticatedSubprocess(self.proxy) as (parent, rpipe):
            if not parent:
               self.schedd.act(htcondor.JobAction.Hold, rootConst)
        results = rpipe.read()
        if results != "OK":
            raise Exception("Failure when killing task: %s" % results)

    def execute(self, *args, **kw):

        try:
            self.executeInternal(*args, **kw)
        except Exception, exc:
            self.logger.error(str(traceback.format_exc()))
            # TODO: fail the task.
        finally:
            if kw['task']['kill_all']:
                configreq = {'workflow': kw['task']['tm_taskname'], 'status': "KILLED"}
                self.server.post(self.resturl, data = urllib.urlencode(configreq))
            else:
                configreq = {'workflow': kw['task']['tm_taskname'], 'status': "SUBMITTED"}
                self.server.post(self.resturl, data = urllib.urlencode(configreq))

