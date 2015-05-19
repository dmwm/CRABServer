import os
import re
import sys
import time
import json
import shutil
import urllib2
import logging
import traceback

from TaskWorker.Actions.Recurring.BaseRecurringAction import BaseRecurringAction

class BanDestinationSites(BaseRecurringAction):
    pollingTime = 15 #minutes

    def _execute(self, resthost, resturi, config, task):
        renewer = CRAB3BanDestinationSites(config, resthost, resturi, self.logger)
        return renewer.execute()

class CRAB3BanDestinationSites(object):

    def __init__(self, config, resthost, resturi, logger=None):
        if not logger:
            self.logger = logging.getLogger(__name__)
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger = logger

        self.config = config

    def writeBannedSitesToFile(self, bannedSites, saveLocation):
        # Create temporary file with save_location.txt.tmp and later we can move it
        tmpLocation = saveLocation + ".tmp"
        with open(tmpLocation, 'w') as fd:
            json.dump(bannedSites, fd)
        shutil.move(tmpLocation, saveLocation) 

    def execute(self):
        blacklistedSites = []
        try:
            usableSites = urllib2.urlopen(self.config.Sites.DashboardURL).read()
        except Exception as e:
            # If exception is got, don`t change anything and previous data will be used
            self.logger.error("Got exception in retrieving usable sites list from %s. Exception: %s" \
                              % (self.config.Sites.DashboardURL, traceback.format_exc()))
            return
        usableSitesList = []
        try:
            usableSitesList = json.loads(usableSites)
        except ValueError as e:
            self.logger.error("Can not load usableSites json. Output %s Error %s" %(bannedSites, e))
            return
        for row in usableSitesList:
            if 'value' in row and row['value'] == 'not_usable':
                blacklistedSites.append(row['name'])
        saveLocation = os.path.join(self.config.TaskWorker.scratchDir, "blacklistedSites.txt")
        self.writeBannedSitesToFile(blacklistedSites, saveLocation)

if __name__ == '__main__':
    """ Simple main to execute the action standalon. You just need to set the task worker environment.
        The main is set up to work with the production task worker. If you want to use it on your own
        instance you need to change twconfig. For example:
            twconfig = '/data/srv/TaskManager/current/TaskWorkerConfig.py'
            resthost = 'hostname' # not used in this script, but required for Recurring action
            resturi = 'resturi' # also not used, but required. 
    """
    twconfig = '/data/srv/TaskManager/current/TaskWorkerConfig.py'
    resthost = 'cmsweb.cern.ch' # Fake resthost and it is not used, but required for Recurring actions
    resturi = 'crabserver' # Fake resturi and it is not used, but required for Recurring actions

    logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", datefmt="%a, %d %b %Y %H:%M:%S %Z(%z)")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    from WMCore.Configuration import loadConfigurationFile
    config = loadConfigurationFile(twconfig)

    pr = CRAB3BanDestinationSites(config, resthost, resturi, logger)
    pr.execute()

