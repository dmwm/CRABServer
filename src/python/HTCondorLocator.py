from __future__ import division
from __future__ import absolute_import
import time
import bisect
import random

import classad
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
    assert total > 0, "all choices have zero weight"
    x = random.random() * total
    i = bisect.bisect(cum_weights, x)
    return values[i]

def filterScheddsByClassAds(schedds, classAds, logger=None):
    """ Check a list of schedds for missing classAds
        Used when choosing a schedd to see if each schedd has the needed classads defined.
        Return a list of valid schedds to choose from
    """

    validSchedds = []

    # Create a list of schedds to be ignored
    for schedd in schedds:
        scheddValid = True
        for classAd in classAds:
            if classAd not in schedd:
                if logger:
                    logger.debug("Ignoring %s schedd since it is missing the %s ClassAd." % (schedd['Name'], classAd))
                scheddValid = False
        if scheddValid:
            validSchedds.append(schedd)

    return validSchedds

def capacityMetricsChoicesHybrid(schedds, logger=None):
    """ Mix of Jadir's way and Marco's way.
        Return a list of scheddobj and the weight to be used in the weighted choice.
    """
    # make sure schedds have the classAds which we will use to further select and weight
    classAdsRequired = ['DetectedMemory', 'TotalFreeMemoryMB', 'MaxJobsRunning', 'TotalRunningJobs',
                        'TransferQueueMaxUploading', 'TransferQueueNumUploading', 'Name', 'IsOK']
    schedds = filterScheddsByClassAds(schedds, classAdsRequired, logger)

    totalMemory = totalJobs = totalUploads = 0
    for schedd in schedds:
        totalMemory += schedd['DetectedMemory']
        totalJobs += schedd['MaxJobsRunning']
        totalUploads += schedd['TransferQueueMaxUploading']

    logger.debug("Total Mem: %d, Total Jobs: %d, Total Uploads: %d" % (totalMemory, totalJobs, totalUploads))
    weights = {}
    for schedd in schedds:
        memPerc = schedd['TotalFreeMemoryMB'] / totalMemory
        jobPerc = (schedd['MaxJobsRunning'] - schedd['TotalRunningJobs']) / totalJobs
        uplPerc = (schedd['TransferQueueMaxUploading'] - schedd['TransferQueueNumUploading']) / totalUploads
        weight = min(memPerc, uplPerc, jobPerc)
        weights[schedd['Name']] = weight
        logger.debug("%s: Mem %d, MemPrct %0.2f, Run %d, RunPrct %0.2f, Trf %d, TrfPrct %0.2f, weight: %f" %
                     (schedd['Name'], schedd['TotalFreeMemoryMB'], memPerc,
                      schedd['JobsRunning'], jobPerc,
                      schedd['TransferQueueNumUploading'], uplPerc, weight))
    choices = [(schedd['Name'], weights[schedd['Name']]) for schedd in schedds]
    return choices

def memoryBasedChoices(schedds, logger=None):
    """ Choose the schedd based on the DetectedMemory classad present in the schedds object
        Return a list of scheddobj and the weight to be used in the weighted choice
    """
    weights = {}
    for schedd in schedds:
        if 'DetectedMemory' in schedd and 'Name' in schedd:
            weight = schedd['DetectedMemory']
        else:
            weight = 24*1024
        weights[schedd['Name']] = weight
    choices = [(schedd['Name'], weights[schedd['Name']]) for schedd in schedds]
    return choices


class HTCondorLocator(object):

    def __init__(self, config, logger=None):
        self.config = config
        self.logger = logger
        self.scheddAd = ""

    def adjustWeights(self, choices):
        """ The method iterates over the htcondorSchedds dict from the REST and ajust schedds
            weights based on the weightfactor key. Negative weightfactors are illegal.
            param choices: a list containing schedds and their weight as ntuples, such as
                        [(u'crab3-5@vocms05.cern.ch', 24576), (u'crab3-5@vocms059.cern.ch', 23460L)]
        """
        i = 0
        for schedd, weight in choices:
            weightfactor = self.config['htcondorSchedds'].get(schedd, {}).get("weightfactor", 1)
            assert weightfactor >= 0 , "Illegal, negative, weightfactor %d found in config"
            newweight = weight * self.config['htcondorSchedds'].get(schedd, {}).get("weightfactor", 1)
            choices[i] = (schedd, newweight)
            i += 1
        return

    def getSchedd(self, chooserFunction=memoryBasedChoices):
        """
        Determine a schedd to use for this task.
        param chooserFunction: name of a function which takes a list of schedds (a n-tuple of classAds each) and
                                returns a list containing schedd names and their weight as n-tuples, such as
                                [(u'crab3-5@vocms05.cern.ch', 24576), (u'crab3-5@vocms059.cern.ch', 23460L)]
        """
        collector = self.getCollector()
        schedd = None

        try:
            htcondor.param['COLLECTOR_HOST'] = collector.encode('ascii', 'ignore')
            coll = htcondor.Collector()
            # select from collector crabschedds and pull some add values
            # this call returns a list of schedd objects.
            schedds = coll.query(htcondor.AdTypes.Schedd, 'CMSGWMS_Type=?="crabschedd"',
                                 ['Name', 'DetectedMemory', 'TotalFreeMemoryMB', 'TransferQueueNumUploading',
                                  'TransferQueueMaxUploading','TotalRunningJobs', 'JobsRunning', 'MaxJobsRunning', 'IsOK'])

            # Get only those schedds that are listed in our external REST configuration
            if self.config and "htcondorSchedds" in self.config:
                schedds = [ schedd for schedd in schedds if schedd['Name'] in self.config['htcondorSchedds']]

            # Keep only those schedds with a non-zero weightfactor in our external REST configuration
            zeroSchedds = []
            for schedd in schedds:
                weightfactor = self.config['htcondorSchedds'].get(schedd['Name'], {}).get("weightfactor", 1)
                if not weightfactor:
                    zeroSchedds.append(schedd['Name'])
            self.logger.debug("Skip these schedds because have a zero weightfactor (maybe in drain) in the REST configuration: %s" % zeroSchedds)
            schedds = [ schedd for schedd in schedds if schedd['Name'] not in zeroSchedds]

            # Keep only those schedds for which the status is OK
            notOkSchedNames = [schedd['Name'] for schedd in schedds if not classad.ExprTree.eval(schedd['IsOk'])]
            if notOkSchedNames:
                self.logger.debug("Skip these schedds because isOK is False: %s" % notOkSchedNames)
                schedds = [schedd for schedd in schedds if schedd['Name'] not in notOkSchedNames]

            # Keep only schedds which can start more jobs in SchedulerUniverse
            saturatedScheds = coll.query(htcondor.AdTypes.Schedd,
                                             'StartSchedulerUniverse =?= false && CMSGWMS_Type=?="crabschedd"',
                                             ['Name'])
            saturatedSchedNames = [sched['Name'] for sched in saturatedScheds]
            if saturatedSchedNames:
                self.logger.debug("Skip these schedds because are at the MaxTask limit: %s" % saturatedSchedNames)
                schedds = [schedd for schedd in schedds if schedd['Name'] not in saturatedSchedNames]

            if schedds:
                self.logger.debug("Will pick best schedd among %s" % [sched['Name'] for sched in schedds])
            else:
                raise Exception("All possible CRAB schedd's are saturated. Try later")

            choices = chooserFunction(schedds, self.logger)
            if not choices:
                raise Exception("List of possible schedds from %s is empty" % chooserFunction)
            self.adjustWeights(choices)
            schedd = weighted_choice(choices)
        except Exception as ex:
            raise Exception("Could not find any schedd to submit to. Exception was raised: %s\n" % str(ex))

        return schedd

    def getScheddObjNew(self, schedd):
        """
        Return a tuple (schedd, address) containing an object representing the
        remote schedd and its corresponding address.
        """
        htcondor.param['COLLECTOR_HOST'] = self.getCollector().encode('ascii', 'ignore')
        coll = htcondor.Collector()
        schedds = coll.query(htcondor.AdTypes.Schedd, 'Name=?=%s' % HTCondorUtils.quote(schedd.encode('ascii', 'ignore')),
                             ["AddressV1", "CondorPlatform", "CondorVersion", "Machine", "MyAddress", "Name", "MyType",
                              "ScheddIpAddr", "RemoteCondorSetup"])
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
        Saves Collector output in a memory cache
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
