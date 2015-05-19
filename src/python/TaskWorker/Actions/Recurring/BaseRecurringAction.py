import time
import logging
from TaskWorker.DataObjects.Result import Result

def handleRecurring(resthost, resturi, config, task, procnum, action):
    actionClass = action.split('.')[-1]
    mod = __import__(action, fromlist=actionClass)
    getattr(mod, actionClass)().execute(resthost, resturi, config, task, procnum)

class BaseRecurringAction:
    def __init__(self):
        self.lastExecution = time.time()
        #set the logger
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            hdlr = logging.FileHandler('logs/recurring.log')
            formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(module)s:%(message)s')
            hdlr.setFormatter(formatter)
            self.logger.addHandler(hdlr)
        self.logger.setLevel(logging.DEBUG)

    def isTimeToGo(self):
        timetogo = time.time() - self.lastExecution > self.pollingTime * 60
        if timetogo:
            self.lastExecution = time.time()
        return timetogo

    def execute(self, resthost, resturi, config, task, procnum):
        try:
            self.logger.info("Executing %s" % task)
            self._execute(resthost, resturi, config, task)
            return Result(task=task['tm_taskname'], result="OK")
        except Exception as ex:
            self.logger.error("Error while runnig recurring action.")
            self.logger.exception(ex)
            return Result(task=task['tm_taskname'], result="KO")
