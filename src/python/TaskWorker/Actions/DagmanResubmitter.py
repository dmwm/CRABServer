
import base64
import urllib
import traceback

import classad
import htcondor

import HTCondorLocator
import HTCondorUtils

import TaskWorker.Actions.TaskAction as TaskAction

class DagmanResubmitter(TaskAction.TaskAction):

    """
    Given a task name, resubmit failed tasks.

    Internally, we simply release the failed DAG.
    """

    def execute_internal(self, *args, **kw):

        if 'task' not in kw:
            raise ValueError("No task specified.")
        task = kw['task']
        if 'tm_taskname' not in task:
            raise ValueError("No taskname specified.")
        workflow = str(task['tm_taskname'])
        if 'user_proxy' not in task:
            raise ValueError("No proxy provided")
        proxy = task['user_proxy']

        self.logger.info("About to resubmit workflow: %s. Getting status first." % workflow)
        self.logger.info("Task info: %s" % str(task))

        loc = HTCondorLocator.HTCondorLocator(self.config)
        scheddName = loc.getSchedd()
        schedd, address = loc.getScheddObj(scheddName)

        # Release the DAG
        rootConst = "TaskType =?= \"ROOT\" && CRAB_ReqName =?= %s" % HTCondorUtils.quote(workflow)

        # Calculate a new white/blacklist
        ad = classad.ClassAd()
        ad['whitelist'] = task['resubmit_site_whitelist']
        ad['blacklist'] = task['resubmit_site_blacklist']

        with HTCondorUtils.AuthenticatedSubprocess(proxy) as (parent, rpipe):
            if not parent:
                schedd.edit(rootConst, "CRAB_SiteBlacklist", ad['blacklist'])
                schedd.edit(rootConst, "CRAB_SiteWhitelist", ad['whitelist'])
                schedd.act(htcondor.JobAction.Release, rootConst)
        results = rpipe.read()
        if results != "OK":
            raise Exception("Failure when killing job: %s" % results)


    def execute(self, *args, **kwargs):

        try:
            return self.execute_internal(*args, **kwargs)
        except Exception, e:
            msg = "Task %s resubmit failed: %s." % (kwargs['task']['tm_taskname'], str(e))
            self.logger.error(msg)
            configreq = {'workflow': kwargs['task']['tm_taskname'],
                         'status': "FAILED",
                         'subresource': 'failure',
                         'failure': base64.b64encode(msg)}
            self.server.post(self.resturl, data = urllib.urlencode(configreq))
            raise
        finally:
            configreq = {'workflow': kwargs['task']['tm_taskname'],
                         'status': "SUBMITTED",
                         'jobset': "-1",
                         'subresource': 'success',}
            self.logger.debug("Setting the task as submitted with %s " % str(configreq))
            data = urllib.urlencode(configreq)
            self.server.post(self.resturl, data = data)


