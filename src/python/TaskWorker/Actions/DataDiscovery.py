from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Run import Run
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON

from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.DataObjects.Result import Result

# TEMPORARY
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
import httplib

class DataDiscovery(TaskAction):
    """I am the abstract class for the data discovery.
       Taking care of generalizing different data discovery
       possibilities. Implementing only a common method to
       return a properly formatted output."""

    def formatOutput(self, task, requestname, datasetfiles, locations):
        """Receives as input the result of the data location
           discovery operations and fill up the WMCore objects."""
        self.logger.debug(" Formatting data discovery output ")
        # TEMPORARY
        secmsmap = {}
        sbj = SiteDBJSON({"key":self.config.TaskWorker.cmskey,
                          "cert":self.config.TaskWorker.cmscert})

        wmfiles = []
        evecounter = 0
        lumicounter = 0
        uniquelumis = set()
        for lfn, infos in datasetfiles.iteritems():
            #the block has not been found or has no locations, continue to the next file
            if not infos['BlockName'] in locations or not locations[infos['BlockName']]:
                self.logger.warning("Skipping %s because its block (%s) has no locations" % (lfn, infos['BlockName']))
                continue

            #if the file is invalid then skip it
            if not infos.get('ValidFile', True):
                self.logger.warning("Skipping invalid file %s" % lfn)
                continue

            wmfile = File(lfn=lfn, events=infos['NumberOfEvents'], size=infos['Size'], checksums=infos['Checksums'])
            wmfile['block'] = infos['BlockName']
            wmfile['locations'] = []
            for se in locations[infos['BlockName']]:
                if se  and se not in secmsmap:
                    self.logger.debug("Translating SE %s" %se)
                    try:
                        secmsmap[se] = sbj.seToCMSName(se)
                    except KeyError, ke:
                        self.logger.error("Impossible translating %s to a CMS name through SiteDB" %se)
                        secmsmap[se] = ''
                    except httplib.HTTPException, ex:
                        self.logger.error("Couldn't map SE to site: %s" % se)
                        print "Couldn't map SE to site: %s" % se
                        print "got problem: %s" % ex
                        print "got another problem: %s" % ex.__dict__
                if se and se in secmsmap:
                    if type(secmsmap[se]) == list:
                        wmfile['locations'].extend(secmsmap[se])
                    else:
                        wmfile['locations'].append(secmsmap[se])
            wmfile['workflow'] = requestname
            evecounter += infos['NumberOfEvents']
            for run, lumis in infos['Lumis'].iteritems():
                #self.logger.debug(' - adding run %d and lumis %s' %(run, lumis))
                wmfile.addRun(Run(run, *lumis))
                for lumi in lumis:
                    uniquelumis.add((run, lumi))
                lumicounter += len(lumis)
            wmfiles.append(wmfile)

        uniquelumis = len(uniquelumis)
        self.logger.debug('Tot events found: %d' % evecounter)
        self.logger.debug('Tot lumis found: %d' % uniquelumis)
        self.logger.debug('Duplicate lumis found: %d' % (lumicounter-uniquelumis))
        self.logger.debug('Tot files found: %d' % len(wmfiles))

        return Result(task=task, result=Fileset(name='FilesToSplit', files = set(wmfiles)))
