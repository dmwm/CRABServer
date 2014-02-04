"""
Modules here can be used as recurring actions, i.e., actions that are executed every N minutes
You just need to add a class like this (*) following in this directory. Then just add the
'RecurringActionClassName' to the config.TaskWorker.recurringActions parameter in the TW config file!

(*)
from TaskWorker.Actions.Recurring.BaseRecurringAction import BaseRecurringAction

class RecurringActionClassName(BaseRecurringAction):
    pollingTime = 30 #minutes. Could be a float number for fractions of minutes

    def _execute(self, instance, resturl, config, task): #note the "_" before the name
       pass #put here the code you want to run every pollingTime minutes!
"""
