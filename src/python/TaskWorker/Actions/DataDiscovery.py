from __future__ import print_function
import os
import json

from WMCore.DataStructs.Run import Run
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.LumiList import LumiList
from TaskWorker.WorkerUtilities import CRICService

from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.DataObjects.Result import Result

class DataDiscovery(TaskAction):  # pylint: disable=abstract-method
    """
    I am the abstract class for the data discovery.
    Taking care of generalizing different data discovery
    possibilities. Implementing only a common method to
    return a properly formatted output.
    """

    def __init__(self, *args, **kwargs):
        TaskAction.__init__(self, *args, **kwargs)
        with self.config.TaskWorker.envForCMSWEB:
            self.resourceCatalog = CRICService(logger=self.logger, configDict={"cacheduration": 1, "pycurl": True, "usestalecache": True})

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
        for lfn, infos in datasetfiles.items():
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
                self.logger.warning("Skipping %s because it has no parents")
                continue
            ## Create a WMCore File object.
            size = infos['FileSize']
            checksums = {'Checksum': infos['Checksum'], 'Adler32': infos['Adler32'], 'Md5': infos['Md5']}
            wmfile = File(lfn=lfn, events=infos['NumberOfEvents'], size=size, checksums=checksums, parents=infos['Parents'])
            wmfile['block'] = infos['BlockName']
            try:
                wmfile['locations'] = self.resourceCatalog.PNNstoPSNs(locations[wmfile['block']])
            except Exception as ex:
                self.logger.error("Impossible translating %s to a CMS name through CMS Resource Catalog",
                                    locations[wmfile['block']])
                self.logger.error("got this exception:\n %s", ex)
                raise
            wmfile['workflow'] = requestname
            event_counter += infos['NumberOfEvents']
            for run, lumis in infos['Lumis'].items():
                datasetLumis.setdefault(run, []).extend(lumis)
                wmfile.addRun(Run(run, *lumis))
                for lumi in lumis:
                    uniquelumis.add((run, lumi))
                lumi_counter += len(lumis)
            wmfiles.append(wmfile)

        if blocksWithNoLocations:
            trimmedList = sorted(list(blocksWithNoLocations))[:3] + ['...']
            msg = ("%d blocks will be skipped because are not completely replicated on DISK: %s" %
                   (len(blocksWithNoLocations), trimmedList))
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
        with open(os.path.join(tempDir, "input_dataset_lumis.json"), "w", encoding='utf-8') as fd:
            json.dump(datasetLumis, fd)
        with open(os.path.join(tempDir, "input_dataset_duplicate_lumis.json"), "w", encoding='utf-8') as fd:
            json.dump(datasetDuplicateLumis, fd)

        return Result(task=task, result=Fileset(name='FilesToSplit', files=set(wmfiles)))
