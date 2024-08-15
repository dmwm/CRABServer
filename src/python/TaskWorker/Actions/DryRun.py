"""

"""
from urllib.parse import urlencode

from TaskWorker.DataObjects.Result import Result
from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.WorkerExceptions import TaskWorkerException


class DryRun(TaskAction):
    """
    Upload an archive containing all files needed to run the task to the Cache (necessary for crab submit --dryrun.)
    """
    def executeInternal(self, *args, **kw):
        task = kw['task']
        update = {'workflow': task['tm_taskname'], 'subresource': 'state', 'status': 'UPLOADED'}
        self.logger.debug('Updating task status: %s', str(update))
        self.crabserver.post(api='workflowdb', data=urlencode(update))
        return Result(task=task, result=args[0])

    def execute(self, *args, **kw):
        try:
            return self.executeInternal(*args, **kw)
        except Exception as e:
            msg = "Failed to run DryRun action for %s; '%s'" % (task['tm_taskname'], str(e))
            raise TaskWorkerException(msg) from e
