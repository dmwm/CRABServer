class TaskWorkerException(Exception):
    """General exception to be returned in case of failures
       by the TaskWorker objects"""
    pass

class ConfigException(TaskWorkerException):
    """Returned in case there are issues with the input
       TaskWorker configuration"""
    exitcode = 4000

class PanDAException(TaskWorkerException):
    """Generic exception interacting with PanDA"""
    exitcode = 5000

class PanDAIdException(PanDAException):
    """Returned in case there are issues with the expected
       behaviour of PanDA id's (def, set)"""
    exitcode = 5001

class NoAvailableSite(PanDAException):
    """In case there is no site available to run the jobs
       use this exception"""
    exitcode = 5002

class WorkerHandlerException(TaskWorkerException):
    """Generic exception in case slave worker action
       crashes"""
    exitcode = 6666

class StopHandler(TaskWorkerException):
    """Exception used in order to stop the handler from
       continuing to work the sequent actions"""
    exitcode = 6667
