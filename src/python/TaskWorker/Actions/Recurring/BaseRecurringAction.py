""" Need a doc string here """
import time
import logging
import os

from TaskWorker.DataObjects.Result import Result

def handleRecurring(resthost, dbInstance, config, task, procnum, action):
    """ hanldes recurring actions """
    actionClass = action.split('.')[-1]
    mod = __import__(action, fromlist=actionClass)
    result = getattr(mod, actionClass)(config.TaskWorker.logsDir).execute(resthost, dbInstance, config, task, procnum)
    return result

class BaseRecurringAction:
    """ base class for all recurring actions """
    def __init__(self, logsDir):
        self.lastExecution = 0
        # set the logger
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            if not os.path.exists(logsDir):
                os.makedirs(logsDir)
            handler = logging.FileHandler(logsDir+'/recurring.log')
            formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(module)s:%(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)

    def isTimeToGo(self):
        """ True if it is time ro execute this action """
        timetogo = time.time() - self.lastExecution > self.pollingTime * 60
        if timetogo:
            self.lastExecution = time.time()
        return timetogo

    def execute(self, resthost, dbinstance, config, task, procnum):  # pylint: disable=unused-argument
        """
        runs a recurring action
        All classes inheriting from this Base class must define the _execute() method
        (note the underscore character in front ! )
        """
        try:
            self.logger.info("Executing %s", task)
            self._execute(config, task)
            return Result(task=task, result="OK")
        except Exception as ex:  # pylint: disable=broad-except
            self.logger.error("Error while runnig recurring action.")
            self.logger.exception(ex)
            return Result(task=task, err="RecurringAction FAILED")
