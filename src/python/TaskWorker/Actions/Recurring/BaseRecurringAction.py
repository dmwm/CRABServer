import time
import logging
from TaskWorker.DataObjects.Result import Result

#need this because of the "PicklingError: Can't pickle <type 'instancemethod'>"
def handleRecurring(instance, resturl, config, task, action):
    actionClass = action.split('.')[-1]
    mod = __import__(action, fromlist=actionClass)
    getattr(mod, actionClass)().execute(instance, resturl, config, task)

class BaseRecurringAction:
    def __init__(self):
        self.logger = logging.getLogger()
        self.lastExecution = time.time()

    def isTimeToGo(self):
        timetogo = time.time() - self.lastExecution > self.pollingTime * 60
        if timetogo:
            self.lastExecution = time.time()
        return timetogo

    def execute(self, instance, resturl, config, task):
        self.logger.info("Executing %s" % task)
        self._execute(instance, resturl, config, task)
        return Result(task=task['tm_taskname'], result="OK")
