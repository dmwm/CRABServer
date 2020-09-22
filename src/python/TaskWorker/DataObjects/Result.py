from TaskWorker.WorkerExceptions import TaskWorkerException

class Result(object):
    """Result of an action. This can potentially be subclassed."""

    def __init__(self, task, result=None, err=None, warn=None):
        """Inintializer

        :arg TaskWorker.DataObjects.Task task: the task the result is referring to
        :arg * result: the result can actually be any needed type
        :arg * err: the error can actually be any needed type
                       (exception, traceback, int, ...)
        :arg str warn: a warning message."""
        if not task: 
            raise TaskWorkerException("Task object missing! Internal error to be fixed.")
        self._task = task
        self._result = result
        self._error = err
        self._warning = warn

    @property
    def task(self):
        """Get the task in question"""
        return self._task

    @property 
    def result(self):
        """Get the result value"""
        return self._result if self._result else None

    @property
    def error(self):
        """Get the error if any"""
        return self._error if self._error else None

    @property
    def warning(self):
        """Get the wanring if any"""
        return self._warning if self._warning else None

    def __str__(self):
        """Use me just to print out in case it is needed to debug"""
        # this is only used in Worker.py where the task name is already part of the message,
        # so here only print the actual result. Up to the called to print out self.task as
        # well if needed.
        msgTail = ""
        if self.result:
            status = 'OK'
            msgTail += "Result = %s\n" %str(self.result)
        if self.warning:
            status = 'WARNING'
            msgTail += "Warning = %s\n" %str(self.warning)
        if self.error:
            status = 'ERROR'
            msgTail += "Error = %s\n" %str(self.error)
        msg = ('Status: %s\n' % status) + msgTail
        return msg
