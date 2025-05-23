""" need a doc string here """
import re
from http.client import HTTPException
from urllib.parse import urlencode

import HTCondorLocator
from ServerUtilities import getColumn, FEEDBACKMAIL

from TaskWorker.DataObjects import Result
from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.WorkerExceptions import TaskWorkerException

import htcondor2 as htcondor
import classad2 as classad


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

        # retrieve full task info to assess if it is in a "killable" status
        data = {'subresource': 'search', 'workflow': self.workflow}
        dictresult, _, _ = self.crabserver.get(api='task', data=data)

        if not getColumn(dictresult, 'tw_name') or not getColumn(dictresult, 'clusterid'):
            self.logger.info("Task %s was not submitted to HTCondor scheduler yet", self.workflow)
            return

        self.logger.info("About to kill workflow: %s.", self.workflow)


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
        taskName = classad.quote(self.workflow)
        const = f'(CRAB_ReqName =?= {taskName} && CRAB_DAGType=?="Job")'

        # Note that we can not send kills for jobs not in queue at this time; we'll need the
        # DAG FINAL node to be fixed and the node status to include retry number.
        self.killAll(const)
        return

    def killAll(self, jobConst):

        """ submit the proper commands to condor """

        # In May 2025 we decided not to allow anymore resubmission of killed tasks
        # https://github.com/dmwm/CRABServer/issues/9076

        taskName = classad.quote(self.workflow)
        rootConst = f'(stringListMember(CRAB_DAGType, "BASE PROCESSING TAIL", " ") && CRAB_ReqName =?= {taskName})'

        # Doing condor_rm of DAGs does not mean that it will remove all jobs, this happens to
        # DAG's submitted via condor_dagman_submit, i.e. sub-dags for automatic splitting,
        # but normal (BASE) DAGs are run by starting condor_dagman inside the dagman_bootstrap_startup.sh script
        # so the dagman scheduler will simply be signaled. In this case it is better to do a condor_rm of
        # all vanilla universe jobs, also to ensure that node_state file reports the DAG_Status as FAILED
        # and this must be done separately

        try:
            self.schedd.act(htcondor.JobAction.Remove, rootConst)
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
