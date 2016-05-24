from __future__ import absolute_import
import bisect
import random
import time
import htcondor

import HTCondorUtils

CollectorCache = {}
# From http://stackoverflow.com/questions/3679694/a-weighted-version-of-random-choice
def weighted_choice(choices):
    values, weights = list(zip(*choices))
    total = 0
    cum_weights = []
    for w in weights:
        total += w
        cum_weights.append(total)
    x = random.random() * total
    i = bisect.bisect(cum_weights, x)
    return values[i]


def memoryBasedChoices(schedds, htcondorSchedds):
    """ Choose the schedd based on the DetectedMemory classad present in the schedds object
        Return a list of scheddobj and the weight to be used in the weighted choice
    """
    schedds_dict = {}
    for schedd in schedds:
        if 'DetectedMemory' in schedd and 'Name' in schedd:
            schedds_dict[schedd['Name']] = schedd['DetectedMemory']

    choices = [(i, schedds_dict.get(i, 24 * 1024)) for i in htcondorSchedds]
    return choices


class HTCondorLocator(object):

    def __init__(self, config):
        self.config = config

    def getSchedd(self, chooserFunction=memoryBasedChoices):
        """
        Determine a schedd to use for this task.
        """
        collector = self.getCollector()
        schedd = "localhost"

        htcondor.param['COLLECTOR_HOST'] = collector
        coll = htcondor.Collector()
        schedds = coll.query(htcondor.AdTypes.Schedd, 'StartSchedulerUniverse =?= true', ['Name', 'DetectedMemory',
                            'TransferQueueNumUploading', 'TransferQueueMaxUploading', 'TotalRunningJobs', 'JobsRunning',
                            'MaxJobsRunning', 'IsOK'])
        if self.config and "htcondorSchedds" in self.config:
            choices = chooserFunction(schedds, self.config['htcondorSchedds'])
        schedd = weighted_choice(choices)
        return schedd

    def getScheddObjNew(self, schedd):
        """
        Return a tuple (schedd, address) containing an object representing the
        remote schedd and its corresponding address.
        """
        htcondor.param['COLLECTOR_HOST'] = self.getCollector()
        coll = htcondor.Collector()
        schedds = coll.query(htcondor.AdTypes.Schedd, 'regexp(%s, Name)' % HTCondorUtils.quote(schedd))
        self.scheddAd = ""
        if not schedds:
            self.scheddAd = self.getCachedCollectorOutput(schedd)
        else:
            self.cacheCollectorOutput(schedd, schedds[0])
            self.scheddAd = self.getCachedCollectorOutput(schedd)
        address = self.scheddAd['MyAddress']
        scheddObj = htcondor.Schedd(self.scheddAd)
        return scheddObj, address

    def cacheCollectorOutput(self, cacheName, output):
        """
        Saves Collector output in tmp directory.
        """
        global CollectorCache
        if cacheName in CollectorCache.keys():
            CollectorCache[cacheName]['ScheddAds'] = output
        else:
            CollectorCache[cacheName] = {}
            CollectorCache[cacheName]['ScheddAds'] = output
        CollectorCache[cacheName]['updated'] = int(time.time())

    def getCachedCollectorOutput(self, cacheName):
        """
        Return cached Collector output if they exist.
        """
        global CollectorCache
        now = int(time.time())
        if cacheName in CollectorCache.keys():
            if (now - CollectorCache[cacheName]['updated']) < 1800:
                return CollectorCache[cacheName]['ScheddAds']
            else:
                raise Exception("Unable to contact the collector and cached results are too old for using.")
        else:
            raise Exception("Unable to contact the collector and cached results does not exist for %s" % cacheName)

    def getCollector(self, name="localhost"):
        """
        Return an object representing the collector given the pool name.
        """
        if self.config and "htcondorPool" in self.config:
            return self.config["htcondorPool"]
        return name

