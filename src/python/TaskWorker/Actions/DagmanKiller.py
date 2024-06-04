""" need a doc string here """
import os
import re
from http.client import HTTPException
from urllib.parse import urlencode

import HTCondorLocator
from ServerUtilities import FEEDBACKMAIL
from TaskWorker.DataObjects import Result
from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.WorkerExceptions import TaskWorkerException

if 'useHtcV2' in os.environ:
    import htcondor2 as htcondor
    import classad2 as classad
else:
    import htcondor
    import classad

WORKFLOW_RE = re.compile("[a-z0-9_]+")

class DagmanKiller(TaskAction):
    """
    Given a task name, kill the corresponding task in HTCondor.

    We do not actually "kill" the task off, but put the DAG on hold.
    """

    def executeInternal(self, *args, **kwargs):  # pylint: disable=unused-argument
        """ this does the killing """
        #Marco: I guess these value errors only happens for development instances
        if 'task' not in kwargs:
            raise ValueError("No task specified.")
        self.task = kwargs['task']  # pylint: disable=attribute-defined-outside-init
        if 'tm_taskname' not in self.task:
            raise ValueError("No taskname specified")
        self.workflow = self.task['tm_taskname']  # pylint: disable=attribute-defined-outside-init
        if 'user_proxy' not in self.task:
            raise ValueError("No proxy provided")
        self.proxy = self.task['user_proxy']  # pylint: disable=attribute-defined-outside-init

        self.logger.info("About to kill workflow: %s.", self.workflow)

        self.workflow = str(self.workflow)  # pylint: disable=attribute-defined-outside-init
        if not WORKFLOW_RE.match(self.workflow):
            raise Exception("Invalid workflow name.")

        # Query HTCondor for information about running jobs and update Dashboard appropriately
        if self.task['tm_collector']:
            self.backendurls['htcondorPool'] = self.task['tm_collector']
        loc = HTCondorLocator.HTCondorLocator(self.backendurls)

        try:
            self.schedd, _ = loc.getScheddObjNew(self.task['tm_schedd'])  # pylint: disable=attribute-defined-outside-init
        except Exception as exp:
            msg = "The CRAB server backend was not able to contact the Grid scheduler."
            msg += " Please try again later."
            msg += f" If the error persists send an e-mail to {FEEDBACKMAIL}."
            msg += f" Message from the scheduler: {exp}"
            self.logger.exception("%s: %s", self.workflow, msg)
            raise TaskWorkerException(msg) from exp

        const = f'CRAB_ReqName =?= {classad.quote(self.workflow)} && TaskType=?="Job"'

        # Note that we can not send kills for jobs not in queue at this time; we'll need the
        # DAG FINAL node to be fixed and the node status to include retry number.
        return self.killAll(const)

    def killAll(self, jobConst):

        """ submit the proper commands to condor """

        # We need to keep ROOT, PROCESSING, and TAIL DAGs in hold until periodic remove kicks in.
        # This is needed in case user wants to resubmit.
        rootConst = f'stringListMember(TaskType, "ROOT PROCESSING TAIL", " ") && CRAB_ReqName =?= {classad.quote(self.workflow)}'

        # Holding DAG job does not mean that it will remove all jobs
        # and this must be done separately
        # --------------------------------------
        # From HTCondor documentation
        # http://research.cs.wisc.edu/htcondor/manual/v8.3/2_10DAGMan_Applications.html#SECTION003107000000000000000
        # --------------------------------------
        # After placing the condor_dagman job on hold, no new node jobs will be submitted,
        # and no PRE or POST scripts will be run. Any node jobs already in the HTCondor queue
        # will continue undisturbed. If the condor_dagman job is left on hold, it will remain
        # in the HTCondor queue after all of the currently running node jobs are finished.
        # --------------------------------------

        try:
            self.schedd.act(htcondor.JobAction.Hold, rootConst)
            self.schedd.act(htcondor.JobAction.Remove, jobConst)
        except  Exception as hte:
            msg = "The CRAB server backend was not able to kill the task,"
            msg += " because the Grid scheduler answered with an error."
            msg += " This is probably a temporary glitch. Please try again later."
            msg += f" If the error persists send an e-mail to {FEEDBACKMAIL}."
            msg += f" Error reason: {hte}"
            raise TaskWorkerException(msg) from hte

    def execute(self, *args, **kwargs):
        """
        The execute method of the DagmanKiller class.
        """
        self.executeInternal(*args, **kwargs)
        try:
            ## AndresT: If a task was in FAILED status before the kill, then the new status
            ## after killing some jobs should be FAILED again, not SUBMITTED. However, in
            ## the long term we would like to introduce a final node in the DAG, and I think
            ## the idea would be that the final node will put the task status into FAILED or
            ## COMPLETED (in the TaskDB) once all jobs are finished. In that case I think
            ## also the status method from HTCondorDataWorkflow would not have to return any
            ## adhoc task status anymore (it would just return what is in the TaskDB) and
            ## that also means that FAILED task status would only be a terminal status that
            ## I guess should not accept a kill (because it doesn't make sense to kill a
            ## task for which all jobs have already finished -successfully or not-).
            configreq = {'subresource': 'state',
                         'workflow': kwargs['task']['tm_taskname'],
                         'status': 'KILLED'}
            self.logger.debug("Setting the task as successfully killed with %s", str(configreq))
            self.crabserver.post(api='workflowdb', data=urlencode(configreq))
        except HTTPException as hte:
            self.logger.error(hte.headers)
            msg = "The CRAB server successfully killed the task,"
            msg += f" but was unable to update the task status to {configreq['status']} in the database."
            msg += " This should be a harmless (temporary) error."
            raise TaskWorkerException(msg) from hte

        return Result.Result(task=kwargs['task'], result='OK')
