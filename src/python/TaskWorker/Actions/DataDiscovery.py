from __future__ import print_function
import os
import json
import logging

from WMCore.DataStructs.Run import Run
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.LumiList import LumiList
from WMCore.Services.CRIC.CRIC import CRIC

from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.DataObjects.Result import Result
from TaskWorker.WorkerExceptions import TaskWorkerException
from ServerUtilities import tempSetLogLevel

class DataDiscovery(TaskAction):
    """
    I am the abstract class for the data discovery.
    Taking care of generalizing different data discovery
    possibilities. Implementing only a common method to
    return a properly formatted output.
    """

    def formatOutput(self, task, requestname, datasetfiles, locations, tempDir):
        """
        Receives as input the result of the data location
        discovery operations and fill up the WMCore objects.
        """
        self.logger.debug(" Formatting data discovery output ")

        wmfiles = []
        event_counter = 0
        lumi_counter = 0
        uniquelumis = set()
        datasetLumis = {}
        blocksWithNoLocations = set()
        ## Loop over the sorted list of files.
        configDict = {"cacheduration": 1, "pycurl": True} # cache duration is in hours
        resourceCatalog = CRIC(logger=self.logger, configDict=configDict)
        # can't affort one message from CRIC per file, unless critical !
        with tempSetLogLevel(logger=self.logger, level=logging.ERROR):
            for lfn, infos in datasetfiles.iteritems():
                ## Skip the file if it is not in VALID state.
                if not infos.get('ValidFile', True):
                    self.logger.warning("Skipping invalid file %s", lfn)
                    continue
                ## Skip the file if the block has not been found or has no locations.
                if not infos['BlockName'] in locations or not locations[infos['BlockName']]:
                    self.logger.warning("Skipping %s because its block (%s) has no locations", lfn, infos['BlockName'])
                    blocksWithNoLocations.add(infos['BlockName'])
                    continue

                if task['tm_use_parent'] == 1 and len(infos['Parents']) == 0:
                    raise TaskWorkerException(
                            "The CRAB3 server backend refuses to submit jobs to the Grid scheduler\n" +
                            "because you specified useParents=True but some your files have no" +
                            "parents.\nExample: " + lfn)
                ## Create a WMCore File object.
                try:
                    size = infos['FileSize']
                    checksums = {'Checksum': infos['Checksum'], 'Adler32': infos['Adler32'], 'Md5': infos['Md5']}
                except:
                    #This is so that the task worker does not crash if an old version of WMCore is used (the interface of an API suddenly changed).
                    # We may want to remove the try/except and the following two lines eventually, but keeping them for the moment so other devels won't be affected
                    #See this WMCore commit: https://github.com/dmwm/WMCore/commit/2afc01ae571390f5fa009dd258be757adac89c28#diff-374b7a6640288184175057234e393e1cL204
                    size = infos['Size']
                    checksums = infos['Checksums']
                wmfile = File(lfn = lfn, events = infos['NumberOfEvents'], size = size, checksums = checksums, parents = infos['Parents'])
                wmfile['block'] = infos['BlockName']
                try:
                    wmfile['locations'] = resourceCatalog.PNNstoPSNs(locations[wmfile['block']])
                except Exception as ex:
                    self.logger.error("Impossible translating %s to a CMS name through CMS Resource Catalog", locations[wmfile['block']] )
                    self.logger.error("got this exception:\n %s", ex)
                    raise
                wmfile['workflow'] = requestname
                event_counter += infos['NumberOfEvents']
                for run, lumis in infos['Lumis'].iteritems():
                    datasetLumis.setdefault(run, []).extend(lumis)
                    wmfile.addRun(Run(run, *lumis))
                    for lumi in lumis:
                        uniquelumis.add((run, lumi))
                    lumi_counter += len(lumis)
                wmfiles.append(wmfile)

        if blocksWithNoLocations:
            msg = "The locations of some blocks (%d) have not been found: %s" % (len(blocksWithNoLocations), list(blocksWithNoLocations))
            self.logger.warning(msg)
        self.uploadWarning(msg, task['user_proxy'], task['tm_taskname'])

        uniquelumis = len(uniquelumis)
        self.logger.debug('Tot events found: %d', event_counter)
        self.logger.debug('Tot lumis found: %d', uniquelumis)
        self.logger.debug('Duplicate lumis found: %d', (lumi_counter - uniquelumis))
        self.logger.debug('Tot files found: %d', len(wmfiles))

        self.logger.debug("Starting to create compact lumilists for input dataset")
        datasetLumiList = LumiList(runsAndLumis=datasetLumis)
        datasetLumis = datasetLumiList.getCompactList()
        datasetDuplicateLumis = datasetLumiList.getDuplicates().getCompactList()
        self.logger.debug("Finished to create compact lumilists for input dataset")
        with open(os.path.join(tempDir, "input_dataset_lumis.json"), "w") as fd:
            json.dump(datasetLumis, fd)
        with open(os.path.join(tempDir, "input_dataset_duplicate_lumis.json"), "w") as fd:
            json.dump(datasetDuplicateLumis, fd)

        return Result(task = task, result = Fileset(name = 'FilesToSplit', files = set(wmfiles)))
