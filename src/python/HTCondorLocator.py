
import random

import classad
import htcondor

import HTCondorUtils

class HTCondorLocator(object):

    def __init__(self, config):
        self.config = config

    def getSchedd(self):
        """
        Determine a schedd to use for this task.
        """
        collector = self.getCollector()
        schedd = "localhost"
        if self.config and "htcondorSchedds" in self.config:
            random.shuffle(self.config["htcondorSchedds"])
            schedd = self.config["htcondorSchedds"][0]
        if collector:
            return "%s:%s" % (schedd, collector)
        return schedd

    def getScheddObj(self, name):
        """
        Return a tuple (schedd, address) containing an object representing the
        remote schedd and its corresponding address.
        """
        info = name.split("_")
        if len(info) > 3:
            name = info[2]
        else:
            name = self.getSchedd()
        if name == "localhost":
            schedd = htcondor.Schedd()
            with open(htcondor.param['SCHEDD_ADDRESS_FILE']) as fd:
                address = fd.read().split("\n")[0]
        else:
            info = name.split(":")
            pool = "localhost"
            if len(info) == 3:
                pool = info[1]
            htcondor.param['COLLECTOR_HOST'] = self.getCollector(pool)
            coll = htcondor.Collector()
            schedds = coll.query(htcondor.AdTypes.Schedd, 'regexp(%s, Name)' % HTCondorUtils.quote(info[0]))
            if not schedds:
                raise Exception("Unable to locate schedd %s" % info[0])
            self.scheddAd = schedds[0]
            address = self.scheddAd['MyAddress']
            schedd = htcondor.Schedd(self.scheddAd)
        return schedd, address

    def getCollector(self, name="localhost"):
        """
        Return an object representing the collector given the pool name.
        """
        if self.config and "htcondorPool" in self.config:
            return self.config["htcondorPool"]
        return name

