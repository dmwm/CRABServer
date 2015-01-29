
import types
import bisect
import random

import classad
import htcondor

import HTCondorUtils

# From http://stackoverflow.com/questions/3679694/a-weighted-version-of-random-choice
def weighted_choice(choices):
    values, weights = zip(*choices)
    total = 0
    cum_weights = []
    for w in weights:
        total += w
        cum_weights.append(total)
    x = random.random() * total
    i = bisect.bisect(cum_weights, x)
    return values[i]

class HTCondorLocator(object):

    def __init__(self, config):
        self.config = config

    def getSchedd(self):
        """
        Determine a schedd to use for this task.
        """
        collector = self.getCollector()
        schedd = "localhost"

        htcondor.param['COLLECTOR_HOST'] = collector
        coll = htcondor.Collector()
        schedds = coll.query(htcondor.AdTypes.Schedd, 'StartSchedulerUniverse =?= true', ['Name', 'DetectedMemory'])
        schedds_dict = {}
        for schedd in schedds:
            if 'DetectedMemory' in schedd and 'Name' in schedd:
                schedds_dict[schedd['Name']] = schedd['DetectedMemory']
        if self.config and "htcondorSchedds" in self.config:
            choices = [(i, schedds_dict.get(i, 24*1024)) for i in self.config["htcondorSchedds"]]
            schedd = weighted_choice(choices)
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

