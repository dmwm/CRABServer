import os
import time
import shutil

from datetime import date

from TaskWorker.Actions.Recurring.BaseRecurringAction import BaseRecurringAction

days = 86400*30
now = time.time()

class RemovetmpDir(BaseRecurringAction):
    pollingTime = 60*24 #minutes

    def _execute(self, resthost, resturi, config, task):
        self.logger.info('Checking for directory older than 30 days..')
        for dirpath, dirnames, filenames in os.walk('/data/srv/tmp'):
            for dir in dirnames:
                timestamp = os.path.getmtime(os.path.join(dirpath,dir))
                if now-days > timestamp:
                    try: 
                        self.logger.info('Removing:')
                        self.logger.info(os.path.join(dirpath,dir))
                        shutil.rmtree(os.path.join(dirpath,dir))
                    except Exception,e:
                        print e
                        pass
                    else: 
                        self.logger.info('Directory removed.')
            
        
