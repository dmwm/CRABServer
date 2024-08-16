"""
Performing the data discovery through CMS DBS service.
"""
# avoid pylint complainging about non-elegfant code . It is old and works. Do not touch.
# pylint: disable=too-many-locals, too-many-branches, too-many-nested-blocks, too-many-statements
import os
import logging
import sys

from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.Services.DBS.DBSErrors import DBSReaderError
from WMCore.Configuration import ConfigurationEx
from WMCore import Lexicon

from RucioUtils import getNativeRucioClient, getTapeRecallUsage

from ServerUtilities import MAX_LUMIS_IN_BLOCK, parseDBSInstance, isDatasetUserDataset
from TaskWorker.WorkerExceptions import TaskWorkerException, TapeDatasetException, SubmissionRefusedException
from TaskWorker.Actions.DataDiscovery import DataDiscovery
from TaskWorker.Actions.RucioActions import RucioAction


class DBSDataDiscovery(DataDiscovery):
    """
    the way TW works, we need a class which implements the execute method
    """

    # disable pylint warning in next line since they refer to conflict with the main()
    # at the bottom of this file which is only used for testing
    def __init__(self, config, crabserver='', procnum=-1, rucioClient=None): # pylint: disable=redefined-outer-name
        DataDiscovery.__init__(self, config, crabserver, procnum)
        self.rucioClient = rucioClient

    def checkDatasetStatus(self, dataset, kwargs):
        """ as the name says """
        res = self.dbs.dbs.listDatasets(dataset=dataset, detail=1, dataset_access_type='*')
        if not res:
            raise SubmissionRefusedException(f"Cannot find dataset {dataset} in {self.dbsInstance} DBS instance")
        if len(res) > 1:
            raise SubmissionRefusedException(f"Found more than one dataset while checking in DBS the status of {dataset}")
        res = res[0]
        #import pprint
        #self.logger.info("Input dataset details: %s", pprint.pformat(res))
        accessType = res['dataset_access_type']
        if accessType != 'VALID':
            # as per Dima's suggestion https://github.com/dmwm/CRABServer/issues/4739
            msgForDeprecDS = "Please contact your physics group if you think the dataset should not be deprecated."
            if kwargs['task']['tm_nonvalid_input_dataset'] != 'T':
                msg = f"CRAB refuses to proceed in getting the details of the dataset {dataset} from DBS"
                msg += f"because the dataset is not 'VALID' but '{accessType}'."
                if accessType == 'DEPRECATED':
                    msg += f" ({msgForDeprecDS})"
                msg += " To allow CRAB to consider a dataset that is not 'VALID', set Data.allowNonValidInputDataset = True in the CRAB configuration."
                msg += " Notice that this will not force CRAB to run over all files in the dataset;"
                msg += " CRAB will still check if there are any valid files in the dataset and run only over those files."
                raise SubmissionRefusedException(msg)
            msg = f"The input dataset {dataset} is not 'VALID' but '{accessType}'."
            msg += " CRAB will check if there are any valid files in the dataset and run only over those files."
            if accessType == 'DEPRECATED':
                msg += f" ({msgForDeprecDS})"
            self.uploadWarning(msg, kwargs['task']['user_proxy'], kwargs['task']['tm_taskname'])


    @staticmethod
    def keepOnlyDiskRSEs(locationsMap):
        """
        get all the RucioStorageElements (RSEs) which are of kind 'Disk'
        locationsMap is a dictionary {block1:[locations], block2:[locations],...}
        """
        diskLocationsMap = {}
        for block, locations in locationsMap.items():
            # as of Sept 2020, tape RSEs ends with _Tape, go for the quick hack
            diskRSEs = [rse for rse in locations if not 'Tape' in rse]
            if  'T3_CH_CERN_OpenData' in diskRSEs:
                diskRSEs.remove('T3_CH_CERN_OpenData') # ignore OpenData until it is accessible by CRAB
            if diskRSEs:
                # at least some locations are disk
                diskLocationsMap[block] = diskRSEs
        locationsMap.clear() # remove all blocks
        locationsMap.update(diskLocationsMap) # add only blocks with disk locations

    def checkBlocksSize(self, blocks):
        """ Make sure no single blocks has too many lumis. See
            https://hypernews.cern.ch/HyperNews/CMS/get/dmDevelopment/2022/1/1/1/1/1/1/2.html
        """
        for block in blocks:
            blockInfo = self.dbs.getDBSSummaryInfo(block=block)
            if blockInfo.get('NumberOfLumis', 0) > MAX_LUMIS_IN_BLOCK:
                msg = f"Block {block} contains more than {MAX_LUMIS_IN_BLOCK} lumis."
                msg += "\nThis blows up CRAB server memory"
                msg += "\nCRAB can only split this by ignoring lumi information. You can do this"
                msg += "\nusing FileBased split algorithm and avoiding any additional request"
                msg += "\nwich may cause lumi information to be looked up. See CRAB FAQ for more info:"
                msg += "\nhttps://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ"
                raise SubmissionRefusedException(msg)

    @staticmethod
    def lumiOverlap(d1, d2):
        """
        This finds if there are common run and lumis in two lists in the form of
        {
        '1': [1,2,3,4,6,7,8,9,10],
        '2': [1,4,5,20]
        }
        """
        commonRuns = d1.keys() & d2.keys()
        if len(commonRuns) == 0:
            return False

        for run in commonRuns:
            commonLumis = set(d1[run]).intersection(set(d2[run]))
            if len(commonLumis) > 0 :
                return True
        return False

    def validateInputUserFiles(self, dataset=None, files=None):
        """ make sure that files in input list match input dataset """
        try:
            for file in files:
                Lexicon.lfn(file)  # will raise if file is not a valid lfn
        except AssertionError as ex:
            raise SubmissionRefusedException(f"input file is not a valid LFN: {file}") from ex
        filesInDataset = self.dbs.listDatasetFiles(datasetPath=dataset)
        for file in files:
            if not file in filesInDataset:
                raise SubmissionRefusedException(f"input file use does not belong to input dataset: {file}")
    @staticmethod
    def validateInputBlocks(dataset=None, blocks=None):
        """ make sure that blocks in input list match input dataset """
        try:
            for block in blocks:
                Lexicon.block(block)  # will raise if file is not a valid lfn
        except AssertionError as ex:
            raise SubmissionRefusedException(f"input block is not a valid block name: {block}") from ex
        for block in blocks:
            if not block.split('#')[0] == dataset:
                raise SubmissionRefusedException(f"input block does not belong to input dataset: {block}")

    def execute(self, *args, **kwargs):
        """
        This is a convenience wrapper around the executeInternal function
        """

        # DBS3 requires X509_USER_CERT to be set - but we don't want to leak that to other modules
        # so use a context manager to set an ad hoc env and restore as soon as
        # executeInternal is over, even if it raises exception
        # NOTE this is legacy from the time we were using X509 to submit to schedd, most likely
        # we can use a single X509 env. throught all of TW. But for now.. be conservative

        with self.config.TaskWorker.envForCMSWEB:
            result = self.executeInternal(*args, **kwargs)

        return result

    def executeInternal(self, *args, **kwargs):  # pylint: disable=unused-argument
        """ this is where things happen """

        self.logger.info("Data discovery with DBS")  # to be changed into debug
        if kwargs['task']['tm_dbs_url']:
            dbsurl = kwargs['task']['tm_dbs_url']
        else:
            dbsurl = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'  # a sensible default
        if hasattr(self.config.Services, 'DBSHostName'):
            hostname = dbsurl.split('//')[1].split('/')[0]
            dbsurl = dbsurl.replace(hostname, self.config.Services.DBSHostName)
        self.logger.info("will connect to DBS at URL: %s", dbsurl)
        self.dbs = DBSReader(dbsurl)
        # with new DBS, we can not get the instance from serverinfo api
        # instead, we parse it from the URL
        # if url is 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
        # then self.dbsInstance needs to be 'prod/global'
        self.dbsInstance = parseDBSInstance(dbsurl)

        self.taskName = kwargs['task']['tm_taskname']           # pylint: disable=W0201
        self.username = kwargs['task']['tm_username']           # pylint: disable=W0201
        self.userproxy = kwargs['task']['user_proxy']           # pylint: disable=W0201
        self.logger.debug("Data discovery through %s for %s", self.dbs, self.taskName)

        inputDataset = kwargs['task']['tm_input_dataset']
        inputBlocks = kwargs['task']['tm_user_config']['inputblocks']
        inputUserFiles = kwargs['task']['tm_user_files']
        if inputBlocks:
            msg = f'Only blocks in "Data.inputBlocks" will be processed ({len(inputBlocks)} blocks).'
            self.uploadWarning(msg, self.userproxy, self.taskName)
            self.logger.info(msg)
            self.validateInputBlocks(dataset=inputDataset, blocks=inputBlocks)
        if inputUserFiles:
            msg = f'Only files in "Data.userInputFiles" will be processed ({len(inputUserFiles)} files).'
            self.uploadWarning(msg, self.userproxy, self.taskName)
            self.logger.info(msg)
            self.validateInputUserFiles(dataset=inputDataset, files=inputUserFiles)
        secondaryDataset = kwargs['task'].get('tm_secondary_input_dataset', None)
        runRange = kwargs['task']['tm_split_args']['runs']
        usePartialDataset = kwargs['task']['tm_user_config']['partialdataset']

        # the isUserDataset flag is used to look for data location in DBS instead of Rucio
        isUserDataset = isDatasetUserDataset(inputDataset, self.dbsInstance)

        self.checkDatasetStatus(inputDataset, kwargs)
        self.logger.info("Input Dataset: %s", inputDataset)
        if secondaryDataset:
            self.checkDatasetStatus(secondaryDataset, kwargs)
            self.logger.info("Secondary Dataset: %s", secondaryDataset)

        try:
            # Get the list of blocks for the locations.
            blocks = self.dbs.listFileBlocks(inputDataset)
            #self.logger.debug("Datablock from DBS: %s ", blocks)
            if inputBlocks:
                blocks = [x for x in blocks if x in inputBlocks]
                #self.logger.debug("Matched inputBlocks: %s ", blocks)
            secondaryBlocks = []
            if secondaryDataset:
                secondaryBlocks = self.dbs.listFileBlocks(secondaryDataset)
        except DBSReaderError as dbsexc:
            # dataset not found in DBS is a known use case
            if str(dbsexc).find('No matching data'):
                raise SubmissionRefusedException(
                    f"CRAB could not find dataset {inputDataset} in this DBS instance: {dbsurl}"
                ) from dbsexc
            raise
        ## Create a map for block's locations: for each block get the list of locations.
        ## Note: listFileBlockLocation() gets first the locations from PhEDEx, and if no
        ## locations are found it gets the original locations from DBS. So it should
        ## never be the case at this point that some blocks have no locations.
        ## locationsMap is a dictionary, key=blockName, value=list of PhedexNodes, example:
        ## {'/JetHT/Run2016B-PromptReco-v2/AOD#b10179dc-3723-11e6-9aa5-001e67abf228':
        # [u'T1_IT_CNAF_Buffer', u'T2_US_Wisconsin', u'T1_IT_CNAF_MSS', u'T2_BE_UCL'],
        ## '/JetHT/Run2016B-PromptReco-v2/AOD#89b03ca6-1dc9-11e6-b567-001e67ac06a0':
        # [u'T1_IT_CNAF_Buffer', u'T2_US_Wisconsin', u'T1_IT_CNAF_MSS', u'T2_BE_UCL']}

        # remove following line when ready to allow user dataset to have locations tracked in Rucio
        useRucioForLocations = not isUserDataset
        # uncomment followint line to look in Rucio first for any dataset, and fall back to DBS origin for USER ones
        # useRucioForLocations = True
        locationsFoundWithRucio = False

        if not useRucioForLocations:
            self.logger.info("Will not use Rucio for this dataset")

        totalSizeBytes = 0
        locationsMap = {}
        partialLocationsMap = {}
        secondaryLocationsMap = {}
        scope = "cms"

        if useRucioForLocations:
            # If the dataset is a USER one, use the Rucio user scope to find it
            # NOTE we need a way to enable users to indicate others user scopes as source
            if isUserDataset:
                scope = f"user.{self.username}"
            self.logger.info("Looking up data location with Rucio in %s scope.", scope)
            try:
                for blockName in list(blocks):
                    partialReplicas = set()
                    fullReplicas = set()
                    response = self.rucioClient.list_dataset_replicas(scope=scope, name=blockName, deep=True)
                    sizeBytes = 0
                    for item in response:
                        # same as complete='y' used for PhEDEx
                        if item['state'].upper() == 'AVAILABLE':
                            fullReplicas.add(item['rse'])
                        else:
                            partialReplicas.add(item['rse'])
                        sizeBytes = item['bytes']
                    if fullReplicas:  # only fill map for blocks which have at least one location
                        locationsMap[blockName] = fullReplicas
                        totalSizeBytes += sizeBytes  # this will be used for tapeRecall
                    if partialReplicas:
                        partialLocationsMap[blockName] = partialReplicas

            except Exception as exc:  # pylint: disable=broad-except
                msg = f"Rucio lookup failed with\n{exc}"
                self.logger.warning(msg)

            if locationsMap:
                locationsFoundWithRucio = True
            else:
                msg = "No locations found with Rucio for this dataset"
                self.logger.warning(msg)

        self.logger.debug("Dataset size in GBytes: %s", totalSizeBytes/1e9)

        if not locationsFoundWithRucio:
            self.logger.info("No locations found with Rucio for %s", inputDataset)
            if isUserDataset:
                self.logger.info("USER dataset. Looking up data locations using origin site in DBS")
                try:
                    # locationsMap is a dictionary {blockName:[RSEs], ...}
                    locationsMap = self.dbs.listFileBlockLocation(list(blocks))
                except Exception as ex:
                    raise TaskWorkerException(
                        "CRAB server could not get file locations from DBS for a USER dataset.\n"+\
                        "This is could be a temporary DBS glitch, please try to submit a new task (resubmit will not work)"+\
                        f" and contact the experts if the error persists.\nError reason: {ex}"
                    ) from ex
                useCernbox = False
                for v in locationsMap.values():
                    if 'T3_CH_CERNBOX' in v:
                        useCernbox = True
                if useCernbox:
                    raise SubmissionRefusedException(
                        "USER dataset is located at T3_CH_CERNBOX, but this location \n"+\
                        "is not available to CRAB jobs."
                    )
            else:
                # datasets other than USER *must* be in Rucio
                raise TaskWorkerException(
                    "CRAB server could not get file locations from Rucio.\n" + \
                    "This is could be a temporary Rucio glitch, please try to submit a new task (resubmit will not work)" + \
                    " and contact the experts if the error persists."
                    )

        if secondaryDataset:
            if secondaryDataset.endswith('USER'):
                self.logger.info("Secondary dataset is USER. Looking up data locations using origin site in DBS")
                try:
                    secondaryLocationsMap = self.dbs.listFileBlockLocation(list(secondaryBlocks))
                except Exception as ex:
                    raise TaskWorkerException(
                        "CRAB server could not get file locations from DBS for secondary dataset of USER tier.\n"+\
                        "This is could be a temporary DBS glitch, please try to submit a new task (resubmit will not work)"+\
                        f" and contact the experts if the error persists.\nError reason: {ex}"
                    ) from ex
            else:
                self.logger.info("Trying data location of secondary dataset blocks with Rucio")
                try:
                    for blockName in list(secondaryBlocks):
                        replicas = set()
                        response = self.rucioClient.list_dataset_replicas(scope=scope, name=blockName, deep=True)
                        for item in response:
                            # same as complete='y' used for PhEDEx
                            if item['state'].upper() == 'AVAILABLE':
                                replicas.add(item['rse'])
                        if replicas:  # only fill map for blocks which have at least one location
                            secondaryLocationsMap[blockName] = replicas
                except Exception as exc:  # pylint: disable=broad-except
                    msg = f"Rucio lookup failed with\n{exc}"
                    self.logger.warning(msg)
            if not secondaryLocationsMap:
                msg = f"No locations found for secondaryDataset {secondaryDataset}."
                raise SubmissionRefusedException(msg)


        # From now on code is not dependent from having used Rucio or PhEDEx

        # make a copy of locationsMap dictionary, so that manipulating it in keeOnlyDiskRSE
        # does not touch blocksWithLocation
        blocksWithLocation = locationsMap.copy().keys()
        secondaryBlocksWithLocation = {} #
        if secondaryDataset:
            secondaryBlocksWithLocation = secondaryLocationsMap.copy().keys()

        # take note of where tapes are
        tapeLocations = set()
        for block, locations in locationsMap.items():
            for rse in locations:
                if 'Tape' in rse:
                    tapeLocations.add(rse)
        # then filter out TAPE locations
        self.keepOnlyDiskRSEs(locationsMap)
        self.keepOnlyDiskRSEs(partialLocationsMap)

        notAllOnDisk = set(locationsMap.keys()) != set(blocksWithLocation)
        requestTapeRecall = notAllOnDisk and not usePartialDataset

        if notAllOnDisk and usePartialDataset:
            msg = "Some blocks are on TAPE only and can not be read."
            msg += "\nYou specified to accept a partial dataset, so all blocks"
            msg += "\n on disk will be processed even if only partially replicated"
            self.logger.warning(msg)
            self.uploadWarning(msg, self.userproxy, self.taskName)
            for block in blocksWithLocation:  # all blocks known to Rucio
                if not block in locationsMap:  # no available disk replica for this block
                    if block in partialLocationsMap:
                        locationsMap[block] = partialLocationsMap[block]  # use partial replicas

        # check for tape recall
        #if set(locationsMap.keys()) != set(blocksWithLocation):
        if requestTapeRecall:
            msg = self.executeTapeRecallPolicy(inputDataset, inputBlocks, totalSizeBytes)
            dataToRecall = inputDataset if not inputBlocks else list(blocksWithLocation)
            self.requestTapeRecall(dataToRecall=dataToRecall, sizeToRecall=totalSizeBytes,
                                   tapeLocations=tapeLocations, system='Rucio', msgHead=msg)

        # will not need lumi info if user has asked for split by file with no run/lumi mask
        splitAlgo = kwargs['task']['tm_split_algo']
        lumiMask = kwargs['task']['tm_split_args']['lumis']

        needLumiInfo = splitAlgo != 'FileBased' or lumiMask != [] or runRange != []
        # secondary dataset access relies on run/lumi info
        if secondaryDataset:
            needLumiInfo = True
        if needLumiInfo:
            self.checkBlocksSize(blocksWithLocation) # Interested only in blocks with locations, 'blocks' may contain invalid ones and trigger an Exception
            if secondaryDataset:
                self.checkBlocksSize(secondaryBlocksWithLocation)
        try:
            filedetails = self.dbs.listDatasetFileDetails(inputDataset, getParents=True, getLumis=needLumiInfo, validFileOnly=0)
            if inputBlocks:
                for key, infos in filedetails.copy().items():
                    if not infos['BlockName'] in inputBlocks:
                        del filedetails[key]
            if inputUserFiles:
                for lfn in filedetails.copy().keys():
                    if not lfn in inputUserFiles:
                        del filedetails[lfn]

            if secondaryDataset:
                moredetails = self.dbs.listDatasetFileDetails(secondaryDataset, getParents=False, getLumis=needLumiInfo, validFileOnly=0)
                self.logger.info("Beginning to match files from secondary dataset")

                for dummyFilename, infos in filedetails.items():
                    primaryLumis = infos['Lumis']
                    infos['Parents'] = []
                    for secfilename, secinfos in moredetails.items():
                        if self.lumiOverlap(primaryLumis,secinfos['Lumis']):
                            infos['Parents'].append(secfilename)

                self.logger.info("Done matching files from secondary dataset")
                kwargs['task']['tm_use_parent'] = 1
        except Exception as ex:
            self.logger.exception(ex)
            raise TaskWorkerException(
                "The CRAB3 server backend could not contact DBS to get the files details (Lumis, events, etc).\n"+\
                "This is could be a temporary DBS glitch. Please try to submit a new task (resubmit will not work)"+\
                f" and contact the experts if the error persists.\nError reason: {ex}"
            ) from ex
        if not filedetails:
            raise SubmissionRefusedException(
                ("Cannot find any file inside the dataset. Please, check your dataset in DAS\n%s\n" +
                 "Aborting submission. Resubmitting your task will not help.") %
                (f"https://cmsweb.cern.ch/das/request?instance={self.dbsInstance}&input=dataset={inputDataset}")
            )

        ## Format the output creating the data structures required by WMCore. Filters out invalid files,
        ## files whose block has no location, and figures out the PSN
        result = self.formatOutput(task=kwargs['task'], requestname=self.taskName,
                                   datasetfiles=filedetails, locations=locationsMap,
                                   tempDir=kwargs['tempDir'])

        if not result.result:
            msg = "All dataset files are either invalid or w/o location"
            if kwargs['task']['tm_use_parent']:
                msg += " or w/o a parent.\n"
            dasUrl = f"https://cmsweb.cern.ch/das/request?instance={self.dbsInstance}&input=dataset={inputDataset}"
            msg += f"Please, check your dataset in DAS, {dasUrl}\n"
            msg += "Aborting submission. Resubmitting your task will not help."
            raise SubmissionRefusedException(msg)

        self.logger.debug("Got %s files", len(result.result.getFiles()))

        # lock input data on disk before final submission
        # (submission can still fail in Splitting step, but it happens rarely and usually users fix
        # a configuration mistake and retry, i.e. they really want these data, so let's lock anyhow

        #  only lock data in CMS scope and only if user did no say "I'll take whatever is on disk"
        if not isUserDataset:
            if inputBlocks:
                dataToLock = inputBlocks
            else:
                if not usePartialDataset:
                    dataToLock = inputDataset
                else:
                    dataToLock = list(locationsMap)  # this is a list of blocks
            try:
                locker = RucioAction(config=self.config, crabserver=self.crabserver,
                                     rucioAcccount='crab_input',
                                     taskName=self.taskName, username=self.username,
                                     logger=self.logger)
                locker.lockData(dataToLock=dataToLock)
            except Exception as e:  # pylint: disable=broad-except
                self.logger.exception("Fatal exception in lockData:\n%s", e)
                msg = 'Locking of input data failed. Details in TaskWorker log. Submit anyhow'
                self.logger.info(msg)
                self.uploadWarning(msg, self.userproxy, self.taskName)
        return result


    def requestTapeRecall(self, dataToRecall=None, sizeToRecall=0,
                          tapeLocations=None, system=None, msgHead=''):
        """
        :param dataToRecall: two formats are possible:
            1. a string with a DBS dataset name
            2. a list of blocks to recall from Tape to Disk
        :param sizeToRecall: an integer of recall total size in bytes.
        :param system: a string identifying the DDM system to use 'Rucio' or 'None'
        :param msgHead: a string with the initial part of a message to be used for exceptions
        :return: nothing: Since data on tape means no submission possible, this function will
            always raise a TaskWorkerException to stop the action flow.
            The exception message contains details and an attempt is done to upload it to TaskDB
            so that crab status can report it
        """
        # SB: DO WE REALLY NEED THIS ? It was a helper when transitioning from DDM to Rucio
        msg = msgHead
        if not isinstance(dataToRecall, str) and not isinstance(dataToRecall, list):
            return
        # The else is needed here after the raise !
        if system == 'Rucio':  # pylint: disable=no-else-raise
            recaller = RucioAction(config=self.config, crabserver=self.crabserver,
                                   rucioAcccount='crab_tape_recall',
                                   taskName=self.taskName, username=self.username,
                                   logger=self.logger)
            recaller.recallData(dataToRecall=dataToRecall, sizeToRecall=sizeToRecall,
                                         tapeLocations=tapeLocations, msgHead=msg)

            msg += "\nThis task will be automatically submitted as soon as the stage-out is completed."
            self.uploadWarning(msg, self.userproxy, self.taskName)
            raise TapeDatasetException(msg)
        else:
            msg += '\nIt is not possible to request a recall from tape.'
            msg += "\nPlease, check DAS (https://cmsweb.cern.ch/das) and make sure the dataset is accessible on DISK."
            raise TaskWorkerException(msg)

    def executeTapeRecallPolicy(self, inputDataset, inputBlocks, totalSizeBytes):
        """
        Execute taperecall policy. If it passes, the method will return
        taperecall `msg` to pass to `self.requestTapeRecall`; otherwise, raise
        an exception.

        :param inputDataset: input dataset
        :type inputDataset: str
        :param inputBlocks: input blocks
        :type inputBlocks: list of str or None
        :param totalSizeBytes: size of input data
        :type totalSizeBytes: int

        :return: task's warning message
        :rtype: str
        """
        dataTier = inputDataset.split('/')[3]
        totalSizeTB = int(totalSizeBytes / 1e12)
        maxTierToBlockRecallSizeTB = getattr(self.config.TaskWorker, 'maxTierToBlockRecallSizeTB', 0)
        maxAnyTierRecallSizeTB = getattr(self.config.TaskWorker, 'maxAnyTierRecallSizeTB', 0)
        maxRecallPerUserTB = getattr(self.config.TaskWorker, 'maxRecallPerUserTB', 100e3)  # defaul to 100PB for testing

        # how much this user is recalling already
        self.logger.debug("Find how much user %s is recalling already", self.username)
        usage = getTapeRecallUsage(self.rucioClient, self.username)  # in Bytes
        usageTB = int(usage / 1e12)
        self.logger.debug("User is using %sTB.", usageTB)
        # is there room  for adding this recall ?
        userHasQuota = (usageTB + totalSizeTB) < maxRecallPerUserTB
        if not userHasQuota:
            # prepare a message to send back
            overQmsg = "\n Recall from tape is not possible at the moment. You need to wait until"
            overQmsg += "\n enough of your ongoing tape recall are completed"
            overQmsg += f"\n  used so far: {usageTB}TB, this request: {totalSizeTB}TB, total: "
            overQmsg += f"{usageTB + totalSizeTB}TB, max allowed: {maxRecallPerUserTB}TB"
        if dataTier in getattr(self.config.TaskWorker, 'tiersToRecall', []) or totalSizeTB < maxAnyTierRecallSizeTB:
            msg = f"Task could not be submitted because not all blocks of dataset {inputDataset} are on DISK"
            if not userHasQuota:
                msg += overQmsg
                raise SubmissionRefusedException(msg)
            msg += "\nWill request a full disk copy for you. See"
            msg += "\n https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ#crab_submit_fails_with_Task_coul"
        elif inputBlocks:
            if totalSizeTB < maxTierToBlockRecallSizeTB:
                msg = "Task could not be submitted because blocks specified in 'Data.inputBlocks' are not on disk."
                if not userHasQuota:
                    msg += overQmsg
                    raise SubmissionRefusedException(msg)
                msg += "\nWill request a disk copy for you. See"
                msg += "\n https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ#crab_submit_fails_with_Task_coul"
            else:
                msg = "Some blocks are on TAPE only and will not be processed."
                msg += "\n'The 'Data.inputBlocks' size is larger than allowed size"\
                       " ({totalSizeTB}TB/{maxTierToBlockRecallSizeTB}TB) "\
                       "to issue automatically recall from TAPE."
                msg += "\nIf you need these blocks, contact Data Transfer team via https://its.cern.ch/jira/browse/CMSTRANSF"
                raise SubmissionRefusedException(msg)
        else:
            msg = "Some blocks are on TAPE only and will not be processed."
            msg += "\nThis dataset is too large for automatic recall from TAPE."
            msg += "\nIf you can do with only a part of the dataset, use Data.inputBlocks configuration."
            msg += "\nIf you need the full dataset, contact Data Transfer team via https://its.cern.ch/jira/browse/CMSTRANSF"
            raise SubmissionRefusedException(msg)
        return msg



if __name__ == '__main__':
    ###
    # Usage: python3 DBSDataDiscovery.py dbs_instance dbsDataset [secondaryDataset]
    # where dbs_instance should be either prod/global or prod/phys03
    #
    # Example: python3 DBSDataDiscovery.py prod/global /MuonEG/Run2016B-23Sep2016-v3/MINIAOD
    ###
    import json
    from ServerUtilities import newX509env

    dbsInstance = sys.argv[1]
    dbsDataset = sys.argv[2]
    blockList = []
    if '#' in dbsDataset:  # accep a block name as input
        blockList = [dbsDataset]
        dbsDataset = dbsDataset.split('#')[0]
    dbsSecondaryDataset = sys.argv[3] if len(sys.argv) == 4 else None  # pylint: disable=invalid-name
    DBSUrl = f"https://cmsweb.cern.ch/dbs/{dbsInstance}/DBSReader/"

    logging.basicConfig(level=logging.DEBUG)

    config = ConfigurationEx()
    config.section_("Services")
    config.section_("TaskWorker")

    # will user service cert as defined for TW
    if "X509_USER_CERT" in os.environ:
        config.TaskWorker.cmscert = os.environ["X509_USER_CERT"]
    else:
        config.TaskWorker.cmscert = '/data/certs/servicecert.pem'
    if "X509_USER_KEY" in os.environ:
        config.TaskWorker.cmskey = os.environ["X509_USER_KEY"]
    else:
        config.TaskWorker.cmskey = '/data/certs/servicekey.pem'
    config.TaskWorker.envForCMSWEB = newX509env(X509_USER_CERT=config.TaskWorker.cmscert,
                                                X509_USER_KEY=config.TaskWorker.cmskey)

    config.TaskWorker.instance = 'prod'
    config.TaskWorker.tiersToRecall = ['AOD', 'AODSIM', 'MINIAOD', 'MINIAODSIM', 'NANOAOD', 'NANOAODSIM']
    config.TaskWorker.maxTierToBlockRecallSizeTB = 10

    config.Services.Rucio_host = 'https://cms-rucio.cern.ch'
    config.Services.Rucio_account = 'crab_server'
    config.Services.Rucio_authUrl = 'https://cms-rucio-auth.cern.ch'
    config.Services.Rucio_caPath = '/etc/grid-security/certificates/'
    config.Services.Rucio_cert = '/data/certs/robotcert.pem'
    config.Services.Rucio_key = '/data/certs/robotkey.pem'

    rucioClient = getNativeRucioClient(config=config, logger=logging.getLogger())

    discovery = DBSDataDiscovery(config=config, rucioClient=rucioClient)
    userConfig = {'partialdataset':False,
                  'inputblocks':blockList
                  }
    discoveryOutput = discovery.execute(task={'tm_nonvalid_input_dataset': 'T', 'tm_use_parent': 0,
                                              'user_proxy': 'None','tm_input_dataset': dbsDataset,
                                              'tm_secondary_input_dataset': dbsSecondaryDataset,
                                              'tm_taskname': 'dummyTaskName',
                                              'tm_username':config.Services.Rucio_account,
                                              'tm_split_algo' : 'automatic',
                                              'tm_split_args' : {'runs':[], 'lumis':[]},
                                              'tm_user_config': userConfig, 'tm_user_files': [],
                                              'tm_dbs_url': DBSUrl},
                                        tempDir='')
    # discoveryOutput.result is a WMCore fileset structure
    fileObjects = discoveryOutput.result.getFiles()
    fileDictionaries = [file.json() for file in fileObjects]
    # each file dictionary has the keys
    # 'last_event', 'first_event', 'lfn', 'locations', 'id', 'checksums', 'events', 'merged', 'size', 'runs', 'parents'
    # runs and parents are sorted, but locations are not
    # To facilitate comparisons, make sure that that everything is sorted
    for aFileDict in fileDictionaries:
        aFileDict['locations'].sort()

    with open('DBSDataDiscoveryOutput.json', 'w', encoding='utf-8') as fh:
        json.dump(fileDictionaries, fh)
    print("\nDataDiscovery output saved as DBSDataDiscoveryOutput.json\n")


#===============================================================================
#    Some interesting datasets for testing
#    dataset = '/DoubleMuon/Run2018B-17Sep2018-v1/AOD'        # on tape
#    dataset = '/DoubleMuon/Run2018B-02Apr2020-v1/NANOAOD'    # isNano
#    dataset = '/DoubleMuon/Run2018B-17Sep2018-v1/MINIAOD'    # parent of above NANOAOD (for secondaryDataset lookup)
#    dataset = '/MuonEG/Run2016B-07Aug17_ver2-v1/AOD'         # no Nano on disk (at least atm)
#    dataset = '/MuonEG/Run2016B-v1/RAW'                      # on tape
#    dataset = '/MuonEG/Run2016B-23Sep2016-v3/MINIAOD'        # no NANO on disk (MINIAOD should always be on disk)
#    dataset = '/GenericTTbar/belforte-Stefano-Test-bb695911428445ed11a1006c9940df69/USER' # USER dataset on prod/phys03
#===============================================================================
