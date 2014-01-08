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
        msg = 'Task = %s\n' %self.task['tm_taskname']
        if self.result:
            msg += "Result = %s\n" %str(self.result)
        if self.error:
            msg += "Error = %s\n" %str(self.error)
        if self.warning:
            msg += "Warning = %s\n" %str(self.warning)
        return msg
