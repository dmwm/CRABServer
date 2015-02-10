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

class RenewMorgueSites(BaseRecurringAction):
    pollingTime = 15 #minutes

    def _execute(self, resthost, resturi, config, task):
        renewer = CRAB3RenewMorgueSites(config, resthost, resturi, self.logger)
        return renewer.execute()

class CRAB3RenewMorgueSites(object):

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

    def write_morgue_list(self, banned_sites, save_location):
        # Create temporary file with save_location.txt.tmp and later we can move it
        tmp_location = save_location + ".tmp"
        with open(tmp_location, 'w') as fd:
            json.dump(banned_sites, fd)
        shutil.move(tmp_location, save_location) 

    def execute(self):
        # Check if we have everything what we need for morgue checking
        if not hasattr(self.config.Sites, 'blacklist_morgue_save_path'):
            self.logger.info("Will not update morgue list because path is not specified there to save.")
            return
        if not hasattr(self.config.Sites, 'blacklist_morgue_url'):
            self.logger.info("Will not update morgue list because blacklist_morgue_url is not defined in TaskWorkerConfig.py.")
            return

        save_location = self.config.Sites.blacklist_morgue_save_path
        current_blacklist = []
        if hasattr(self.config.Sites, 'banned'):
            current_blacklist = self.config.Sites.banned

        # If save_location already is a file, merge it into current_blacklist
        if os.path.isfile(save_location):
            with open(save_location, 'r') as fd:
                current_morgue_list = json.load(fd)
                current_blacklist += current_morgue_list

        morgue_url = self.config.Sites.blacklist_morgue_url
        morgue_output = ""
        try:
            morgue_output = urllib2.urlopen(morgue_url).read()
        except Exception, e:
            # If exception is got, don`t change anything and previous data will be used
            self.logger.error("Got exception in retrieving morgue list. Exception: %s" % traceback.format_exc())
            return

        # remove python style comments
        morgue_output = re.sub(re.compile(r'#.*$', re.MULTILINE), "", morgue_output)
        # this function has been written in order to parse metrics from dashboard
        rows = re.findall(r'(.*?)\t(.*?)\t(.*?)\t(.*?)\t(.*?)\n', morgue_output, re.M)
        found_new_morgue = False
        for row in rows:
            # 0 - date, 1 - name, 2 - value, 3 - color, 4 - url
            if row[2] == "in" and row[1] not in current_blacklist:
                self.logger.info("Blocking site which is not in the banned list: %s " % row[1])
                current_blacklist.append(row[1])
                found_new_morgue = True
        #Even if it is one time writing, no need to write it everytime. Write only if we found new morgue site.
        if found_new_morgue:
            self.write_morgue_list(current_blacklist, save_location)
                
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

    pr = CRAB3RenewMorgueSites(config, resthost, resturi, logger)
    pr.execute()
