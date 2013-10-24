
import random

import classad
import htcondor

class HTCondorLocator(object):

    def __init__(self, config):
        self.config = config

    def getSchedd(self):
        """
        Determine a schedd to use for this task.
        """
        collector = self.getCollector()
        schedd = "localhost"
        if self.config and hasattr(self.config, 'TaskWorker') and hasattr(self.config.TaskWorker, 'htcondorSchedds'):
            random.shuffle(self.config.TaskWorker.htcondorSchedds)
            schedd = self.config.TaskWorker.htcondorSchedds[0]
        if collector:
            return "%s:%s" % (schedd, collector)
        return schedd

    def getScheddObj(self, name):
        """
        Return a tuple (schedd, address) containing an object representing the
        remote schedd and its corresponding address.
        """
        if name == "localhost":
            schedd = htcondor.Schedd()
            with open(htcondor.param['SCHEDD_ADDRESS_FILE']) as fd:
                address = fd.read().split("\n")[0]
        else:
            info = name.split(":")
            pool = "localhost"
            if len(info) == 2:
                pool = info[1]
            coll = htcondor.Collector(self.getCollector(pool))
            scheddAd = coll.locate(htcondor.DaemonTypes.Schedd, info[0])
            address = scheddAd['MyAddress']
            schedd = htcondor.Schedd(scheddAd)
        return schedd, address

    def getCollector(self, name="localhost"):
        """
        Return an object representing the collector given the pool name.
        """
        if self.config and hasattr(self.config, 'TaskWorker') and hasattr(self.config.TaskWorker, 'htcondorPool'):
            return self.config.TaskWorker.htcondorPool
        return name

