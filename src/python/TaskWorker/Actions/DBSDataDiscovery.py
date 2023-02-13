from __future__ import print_function
import os
import random
import logging
import copy
from http.client import HTTPException

import sys
from urllib.parse import urlencode  # pylint: disable=no-name-in-module  # for pylint2 compat.

from WMCore.DataStructs.LumiList import LumiList
from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.Services.DBS.DBSErrors import DBSReaderError

from TaskWorker.WorkerExceptions import TaskWorkerException, TapeDatasetException
from TaskWorker.Actions.DataDiscovery import DataDiscovery
from ServerUtilities import FEEDBACKMAIL, parseDBSInstance, isDatasetUserDataset
from RucioUtils import getNativeRucioClient

from rucio.common.exception import (DuplicateRule, DataIdentifierAlreadyExists, DuplicateContent,
                                    InsufficientTargetRSEs, InsufficientAccountLimit, FullStorage)

from dbs.exceptions.dbsClientException import dbsClientException


class DBSDataDiscovery(DataDiscovery):
    """Performing the data discovery through CMS DBS service.
    """

    # disable pylint warning in next line since they refer to conflict with the main()
    # at the bottom of this file which is only used for testing
    def __init__(self, config, crabserver='', procnum=-1, rucioClient=None): # pylint: disable=redefined-outer-name
        DataDiscovery.__init__(self, config, crabserver, procnum)
        self.rucioClient = rucioClient

    def checkDatasetStatus(self, dataset, kwargs):
        res = self.dbs.dbs.listDatasets(dataset=dataset, detail=1, dataset_access_type='*')
        if not res:
            raise TaskWorkerException("Cannot find dataset %s in %s DBS instance" % (dataset, self.dbsInstance))
        if len(res) > 1:
            raise TaskWorkerException("Found more than one dataset while checking in DBS the status of %s" % dataset)
        res = res[0]
        #import pprint
        #self.logger.info("Input dataset details: %s", pprint.pformat(res))
        accessType = res['dataset_access_type']
        if accessType != 'VALID':
            # as per Dima's suggestion https://github.com/dmwm/CRABServer/issues/4739
            msgForDeprecDS = "Please contact your physics group if you think the dataset should not be deprecated."
            if kwargs['task']['tm_nonvalid_input_dataset'] != 'T':
                msg = "CRAB refuses to proceed in getting the details of the dataset %s from DBS, because the dataset is not 'VALID' but '%s'." % (dataset, accessType)
                if accessType == 'DEPRECATED':
                    msg += " (%s)" % (msgForDeprecDS)
                msg += " To allow CRAB to consider a dataset that is not 'VALID', set Data.allowNonValidInputDataset = True in the CRAB configuration."
                msg += " Notice that this will not force CRAB to run over all files in the dataset;"
                msg += " CRAB will still check if there are any valid files in the dataset and run only over those files."
                raise TaskWorkerException(msg)
            msg = "The input dataset %s is not 'VALID' but '%s'." % (dataset, accessType)
            msg += " CRAB will check if there are any valid files in the dataset and run only over those files."
            if accessType == 'DEPRECATED':
                msg += " %s" % (msgForDeprecDS)
            self.uploadWarning(msg, kwargs['task']['user_proxy'], kwargs['task']['tm_taskname'])
        return

    def keepOnlyDiskRSEs(self, locationsMap):
        # get all the RucioStorageElements (RSEs) which are of kind 'Disk'
        # locationsMap is a dictionary {block1:[locations], block2:[locations],...}
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
        """ Make sure no single blocks has more than 100k lumis. See
            https://hypernews.cern.ch/HyperNews/CMS/get/dmDevelopment/2022/1/1/1/1/1/1/2.html
        """
        MAX_LUMIS = 100000
        for block in blocks:
            blockInfo = self.dbs.getDBSSummaryInfo(block=block)
            if blockInfo.get('NumberOfLumis', 0) > MAX_LUMIS:
                msg = "Block %s contains more than %s lumis.\nThis blows up CRAB server memory" % (block, MAX_LUMIS)
                msg += "\nCRAB can only split this by ignoring lumi information. You can do this"
                msg += "\nusing FileBased split algorithm and avoiding any additional request"
                msg += "\nwich may cause lumi information to be looked up. See CRAB FAQ for more info:"
                msg += "\nhttps://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ"
                raise TaskWorkerException(msg)

    def requestTapeRecall(self, blockList=[], system='Dynamo', msgHead=''):   # pylint: disable=W0102
        """
        :param blockList: a list of blocks to recall from Tape to Disk
        :param system: a string identifying the DDM system to use 'Dynamo' or 'Rucio' or 'None'
        :param msgHead: a string with the initial part of a message to be used for exceptions
        :return: nothing: Since data on tape means no submission possible, this function will
            always raise a TaskWorkerException to stop the action flow.
            The exception message contains details and an attempt is done to upload it to TaskDB
            so that crab status can report it
        """
        msg = msgHead
        if system == 'Rucio':
            # need to use crab_tape_recall Rucio account to create containers and create rules
            tapeRecallConfig = copy.copy(self.config)
            tapeRecallConfig.Services.Rucio_account = 'crab_tape_recall'
            rucioClient = getNativeRucioClient(tapeRecallConfig, self.logger) # pylint: disable=redefined-outer-name
            # turn input CMS blocks into Rucio dids in cms scope
            dids = [{'scope': 'cms', 'name': block} for block in blockList]
            # prepare container /TapeRecall/taskname/USER in the service scope
            myScope = 'user.crab_tape_recall'
            containerName = '/TapeRecall/%s/USER' % self.taskName.replace(':', '.')
            containerDid = {'scope':myScope, 'name':containerName}
            self.logger.info("Create RUcio container %s", containerName)
            try:
                rucioClient.add_container(myScope, containerName)
            except DataIdentifierAlreadyExists:
                self.logger.debug("Container name already exists in Rucio. Keep going")
            except Exception as ex:
                msg += "\nRucio exception creating container: %s" %  (str(ex))
                raise TaskWorkerException(msg)
            try:
                rucioClient.attach_dids(myScope, containerName, dids)
            except DuplicateContent:
                self.logger.debug("Some dids are already in this container. Keep going")
            except Exception as ex:
                msg += "\nRucio exception adding blocks to container: %s" %  (str(ex))
                raise TaskWorkerException(msg)
            self.logger.info("Rucio container %s:%s created with %d blocks", myScope, containerName, len(blockList))

            # Compute size of recall request
            sizeToRecall = 0
            for block in blockList:
                replicas = rucioClient.list_dataset_replicas(scope='cms', name=block, deep=True)
                blockBytes = next(replicas)['bytes']  # pick first replica for each block, they better all have same size
                sizeToRecall += blockBytes
            TBtoRecall = sizeToRecall // 1e12
            # Sanity check
            if TBtoRecall > 1e3:
                msg += '\nDataset size %d TB. Will not trigger autoamatic recall for >1PB. Contact DataOps' % TBtoRecall
                raise TaskWorkerException(msg)
            if TBtoRecall > 0:
                self.logger.info("Total size of data to recall : %d TBytes", TBtoRecall)
            else:
                self.logger.info("Total size of data to recall : %d GBytes", sizeToRecall/1e9)

            # make RSEs lists
            # asking for ddm_quota>0 gets rid also of Temp and Test RSE's
            ALL_RSES = "ddm_quota>0&(tier=1|tier=2)&rse_type=DISK"
            rses = rucioClient.list_rses(ALL_RSES)
            rseNames = [r['rse'] for r in rses]
            largeRSEs = []  # a list of largish (i.e. solid) RSEs
            freeRSEs = []  # a subset of largeRSEs which also have quite some free space
            for rse in rseNames:
                if rse[2:6] == '_RU_':  # avoid fragile sites, see #7400
                    continue
                usageDetails = list(rucioClient.get_rse_usage(rse, filters={'source': 'static'}))
                if not usageDetails:  # some RSE's are put in the system with an empty list here, e.g. T2_FR_GRIF
                    continue
                size = usageDetails[0]['used']  # bytes
                if float(size)/1.e15 > 1.0:  # more than 1 PB
                    largeRSEs.append(rse)
                    availableSpace = int(rucioClient.list_rse_attributes(rse)['ddm_quota'])  # bytes
                    if availableSpace/1e12 > 100:  # at least 100TB available
                        freeRSEs.append(rse)
            # use either list (largeRSEs or freeRSEs) according to dataset size:
            if TBtoRecall < 50.:
                grouping = 'ALL'
                self.logger.info("Will place all blocks at a single site")
                RSE_EXPRESSION = '|'.join(largeRSEs)  # any solid site will do, most datasets are a few TB anyhow
            else:
                grouping = 'DATASET'  # Rucio DATASET i.e. CMS block !
                self.logger.info("Will scatter blocks on multiple sites")
                # restrict the list to as many sites as there are 50TB chunks in the dataset, plus some slack
                nRSEs = int(TBtoRecall/50) + 1
                myRSEs = random.sample(freeRSEs, nRSEs)
                RSE_EXPRESSION = '|'.join(myRSEs)
            self.logger.debug('Will use RSE_EXPRESSION = %s', RSE_EXPRESSION)

            # create rule
            #RSE_EXPRESSION = 'ddm_quota>0&(tier=1|tier=2)&rse_type=DISK'
            #RSE_EXPRESSION = 'T3_IT_Trieste' # for testing
            WEIGHT = 'ddm_quota'
            #WEIGHT = None # for testing
            LIFETIME = 14 * 24 * 3600  # 14 days
            ASK_APPROVAL = False
            #ASK_APPROVAL = True # for testing
            ACCOUNT = 'crab_tape_recall'
            copies = 1
            try:
                ruleId = rucioClient.add_replication_rule(
                    dids=[containerDid], copies=copies, rse_expression=RSE_EXPRESSION,
                    grouping=grouping, weight=WEIGHT, lifetime=LIFETIME, account=ACCOUNT,
                    activity='Analysis Input', comment='Staged from tape for %s' % self.username,
                    ask_approval=ASK_APPROVAL, asynchronous=True)
            except DuplicateRule as ex:
                # handle "A duplicate rule for this account, did, rse_expression, copies already exists"
                # which should only happen when testing, since container name is unique like task name, anyhow...
                self.logger.debug("A duplicate rule for this account, did, rse_expression, copies already exists. Use that")
                # find the existing rule id
                ruleId = rucioClient.list_did_rules(myScope, containerName)
            except (InsufficientTargetRSEs, InsufficientAccountLimit, FullStorage) as ex:
                msg = "\nNot enough global quota to issue a tape recall request. Rucio exception:\n%s" % str(ex)
                raise TaskWorkerException(msg)
            except Exception as ex:
                msg += "\nRucio exception creating rule: %s" %  str(ex)
                raise TaskWorkerException(msg)
            ruleId = str(ruleId[0])  # from list to singleId and remove unicode

            msg += "\nA disk replica has been requested to Rucio (rule ID: %s )" % ruleId
            msg += "\nyou can check progress via either of the following two commands:"
            msg += "\n rucio rule-info %s" % ruleId
            msg += "\n rucio list-rules %s:%s" % (myScope, containerName)
            msg += "\nor simply check 'state' line in this page: https://cms-rucio-webui.cern.ch/rule?rule_id=%s" % ruleId
            automaticTapeRecallIsImplemented = True
            if automaticTapeRecallIsImplemented:
                tapeRecallStatus = 'TAPERECALL'
            else:
                tapeRecallStatus = 'SUBMITFAILED'
            configreq = {'workflow': self.taskName,
                         'taskstatus': tapeRecallStatus,
                         'ddmreqid': ruleId,
                         'subresource': 'addddmreqid',
                         }
            try:
                tapeRecallStatusSet = self.crabserver.post(api='task', data=urlencode(configreq))
            except HTTPException as hte:
                self.logger.exception(hte)
                msg = "HTTP Error while contacting the REST Interface %s:\n%s" % (
                    self.config.TaskWorker.restHost, str(hte))
                msg += "\nStoring of %s status and ruleId (%s) failed for task %s" % (
                    tapeRecallStatus, ruleId, self.taskName)
                msg += "\nHTTP Headers are: %s" % hte.headers
                raise TaskWorkerException(msg, retry=True)
            if tapeRecallStatusSet[2] == "OK":
                self.logger.info("Status for task %s set to '%s'", self.taskName, tapeRecallStatus)
            if automaticTapeRecallIsImplemented:
                msg += "\nThis task will be automatically submitted as soon as the stage-out is completed."
                self.uploadWarning(msg, self.userproxy, self.taskName)
                raise TapeDatasetException(msg)
            # fall here if could not setup for automatic submission after recall
            msg += "\nPlease monitor recall progress via Rucio or DAS and try again once data are on disk."
            raise TaskWorkerException(msg)

        if system == 'None':
            msg += '\nIt is not possible to request a recall from tape.'
            msg += "\nPlease, check DAS (https://cmsweb.cern.ch/das) and make sure the dataset is accessible on DISK."
            raise TaskWorkerException(msg)

        if system == 'Dynamo':
            raise NotImplementedError

    def getBlocksSizeBytes(self, dataset, blocks=None):
        """
        Get block size of dataset from DBS and return the total size of all blocks or blocks in `blocks` argument.
        """
        self.dbs.checkDatasetPath(dataset)
        args = {'dataset': dataset, 'detail': True}
        try:
            blocksSummaries = self.dbs.dbs.listBlockSummaries(**args)
        except dbsClientException as ex:
            msg = "Error in DBSReader.listFileBlocks(%s)\n" % dataset
            msg += "%s\n" % ex
            raise DBSReaderError(msg) from None

        size = 0
        for block in blocksSummaries:
            if not blocks or block['block_name'] in blocks:
                size += block['file_size']
        return size

    def execute(self, *args, **kwargs):
        """
        This is a convenience wrapper around the executeInternal function
        """

        # DBS3 requires X509_USER_CERT to be set - but we don't want to leak that to other modules
        # so use a context manager to set an ad hoc env and restore as soon as
        # executeInternal is over, even if it raises exception

        with self.config.TaskWorker.envForCMSWEB:
            result = self.executeInternal(*args, **kwargs)

        return result

    def executeInternal(self, *args, **kwargs):


        self.logger.info("Data discovery with DBS") ## to be changed into debug


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
        if inputBlocks:
            msg = f'Only blocks in "Data.inputBlocks" will be processed ({len(inputBlocks)} blocks).'
            self.uploadWarning(msg, self.userproxy, self.taskName)
            self.logger.info(msg)
        secondaryDataset = kwargs['task'].get('tm_secondary_input_dataset', None)

        # the isUserDataset flag is used to look for data location in DBS instead of Rucio
        isUserDataset = isDatasetUserDataset(inputDataset, self.dbsInstance)

        self.checkDatasetStatus(inputDataset, kwargs)
        if secondaryDataset:
            self.checkDatasetStatus(secondaryDataset, kwargs)

        try:
            # Get the list of blocks for the locations.
            blocks = self.dbs.listFileBlocks(inputDataset)
            self.logger.debug("Datablock from DBS: %s ", blocks)
            if inputBlocks:
                blocks = [x for x in blocks if x in inputBlocks]
                self.logger.debug("Matched inputBlocks: %s ", blocks)
            if secondaryDataset:
                secondaryBlocks = self.dbs.listFileBlocks(secondaryDataset)
        except DBSReaderError as dbsexc:
            # dataset not found in DBS is a known use case
            if str(dbsexc).find('No matching data'):
                raise TaskWorkerException("CRAB could not find dataset %s in this DBS instance: %s" % inputDataset, dbsurl)
            raise
        ## Create a map for block's locations: for each block get the list of locations.
        ## Note: listFileBlockLocation() gets first the locations from PhEDEx, and if no
        ## locations are found it gets the original locations from DBS. So it should
        ## never be the case at this point that some blocks have no locations.
        ## locationsMap is a dictionary, key=blockName, value=list of PhedexNodes, example:
        ## {'/JetHT/Run2016B-PromptReco-v2/AOD#b10179dc-3723-11e6-9aa5-001e67abf228': [u'T1_IT_CNAF_Buffer', u'T2_US_Wisconsin', u'T1_IT_CNAF_MSS', u'T2_BE_UCL'],
        ## '/JetHT/Run2016B-PromptReco-v2/AOD#89b03ca6-1dc9-11e6-b567-001e67ac06a0': [u'T1_IT_CNAF_Buffer', u'T2_US_Wisconsin', u'T1_IT_CNAF_MSS', u'T2_BE_UCL']}

        # remove following line when ready to allow user dataset to have locations tracked in Rucio
        useRucioForLocations = not isUserDataset
        # uncomment followint line to look in Rucio first for any dataset, and fall back to DBS origin for USER ones
        # useRucioForLocations = True
        locationsFoundWithRucio = False

        if not useRucioForLocations:
            self.logger.info("Will not use Rucio for this dataset")

        if useRucioForLocations:
            scope = "cms"
            # If the dataset is a USER one, use the Rucio user scope to find it
            # TODO: we need a way to enable users to indicate others user scopes as source
            if isUserDataset:
                scope = "user.%s" % self.username
            self.logger.info("Looking up data location with Rucio in %s scope.", scope)
            locationsMap = {}
            try:
                for blockName in list(blocks):
                    replicas = set()
                    response = self.rucioClient.list_dataset_replicas(scope=scope, name=blockName, deep=True)
                    for item in response:
                        if 'T2_UA_KIPT' in item['rse']:
                            continue  # skip Ucrainan T2 until further notice
                        # same as complete='y' used for PhEDEx
                        if item['state'].upper() == 'AVAILABLE':
                            replicas.add(item['rse'])
                    if replicas:  # only fill map for blocks which have at least one location
                        locationsMap[blockName] = replicas
            except Exception as exc:
                msg = "Rucio lookup failed with\n%s" % str(exc)
                self.logger.warning(msg)
                locationsMap = None

            if locationsMap:
                locationsFoundWithRucio = True
            else:
                msg = "No locations found with Rucio for this dataset"
                self.logger.warning(msg)

        if not locationsFoundWithRucio:
            self.logger.info("No locations found with Rucio for %s", inputDataset)
            if isUserDataset:
                self.logger.info("USER dataset. Looking up data locations using origin site in DBS")
                try:
                    locationsMap = self.dbs.listFileBlockLocation(list(blocks))
                except Exception as ex:
                    raise TaskWorkerException(
                        "CRAB server could not get file locations from DBS for a USER dataset.\n"+\
                        "This is could be a temporary DBS glitch, please try to submit a new task (resubmit will not work)"+\
                        " and contact the experts if the error persists.\nError reason: %s" % str(ex)
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
                        " and contact the experts if the error persists.\nError reason: %s" % str(ex)
                    )
            else:
                self.logger.info("Trying data location of secondary dataset blocks with Rucio")
                secondaryLocationsMap = {}
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
                except Exception as exc:
                    msg = "Rucio lookup failed with\n%s" % str(exc)
                    self.logger.warning(msg)
                    secondaryLocationsMap = None
            if not secondaryLocationsMap:
                msg = "No locations found for secondaryDataset %s." % secondaryDataset
                raise TaskWorkerException(msg)


        # From now on code is not dependent from having used Rucio or PhEDEx

        # make a copy of locationsMap dictionary, so that manipulating it in keeOnlyDiskRSE
        # does not touch blocksWithLocation
        blocksWithLocation = locationsMap.copy().keys()
        if secondaryDataset:
            secondaryBlocksWithLocation = secondaryLocationsMap.copy().keys()

        # filter out TAPE locations
        # temporary hack until we have a new config. parameter: allow user to accpet a partial dataset
        # by inserting '0' as (first element of) runRange
        usePartialDataset = False
        runRange = kwargs['task']['tm_split_args']['runs']
        if runRange and runRange[0] == '0':
            usePartialDataset = True
            # remove initial '0' from run list
            # note that it caused an extra entry to be created in lumilis as well
            kwargs['task']['tm_split_args']['runs'] = kwargs['task']['tm_split_args']['runs'][1:]
            kwargs['task']['tm_split_args']['lumis'] = kwargs['task']['tm_split_args']['lumis'][1:]
            runRange = kwargs['task']['tm_split_args']['runs']
        # or use Data.partialDataset configuration option in client
        elif kwargs['task']['tm_user_config']['partialdataset']:
            usePartialDataset = True

        self.keepOnlyDiskRSEs(locationsMap)
        if set(locationsMap.keys()) != set(blocksWithLocation):
            dataTier = inputDataset.split('/')[3]
            if usePartialDataset:
                msg = "Some blocks are on TAPE only and can not be read."
                msg += "\nSince you specified to accept a partial dataset, only blocks on disk will be processed"
                self.logger.warning(msg)
                self.uploadWarning(msg, self.userproxy, self.taskName)
            elif dataTier in getattr(self.config.TaskWorker, 'tiersToRecall', []):
                msg = "Task could not be submitted because not all blocks of dataset %s are on DISK" % inputDataset
                msg += "\nWill try to request a full disk copy for you. See"
                msg += "\n https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ#crab_submit_fails_with_Task_coul"
                self.requestTapeRecall(blockList=blocksWithLocation, system='Rucio', msgHead=msg)
            elif inputBlocks:
                blocksSizeToRecall = self.getBlocksSizeBytes(inputDataset, list(blocksWithLocation))
                maxTierToBlockRecallSizeTB = getattr(self.config.TaskWorker, 'maxTierToBlockRecallSizeTB', 0)
                maxTierToBlockRecallSize = maxTierToBlockRecallSizeTB * 1e12
                if blocksSizeToRecall < maxTierToBlockRecallSize:
                    msg = "Task could not be submitted because blocks specified in Data.inputBlocks are not on disk."
                    msg += "\nWill try to request disk copy for you. See"
                    msg += "\n https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ#crab_submit_fails_with_Task_coul"
                    self.requestTapeRecall(blockList=blocksWithLocation, system='Rucio', msgHead=msg)
                else:
                    msg = "Some blocks are on TAPE only and will not be processed."
                    msg += f"\nThere is no automatic recall from TAPE for data tier '{datatier}' if 'Data.inputBlocks' is provided,"
                    msg += f"\nbut the recall size ({blocksSizeToRecall/1e12:.3f} TB) is larger than the maximum allowed size ({maxTierToBlockRecallSizeTB} TB)."
                    msg += '\nIf you need these blocks, contact Data Transfer team via %s' % FEEDBACKMAIL
                    self.logger.warning(msg)
                    self.uploadWarning(msg, self.userproxy, self.taskName)
            else:
                msg = "Some blocks are on TAPE only and will not be processed."
                msg += "\nThere is no automatic recall from tape for data tier '%s' if 'Data.inputBlocks' is not provided." % dataTier
                msg += '\nIf you need the full dataset, contact Data Transfer team via %s' % FEEDBACKMAIL
                self.logger.warning(msg)
                self.uploadWarning(msg, self.userproxy, self.taskName)

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
            if secondaryDataset:
                moredetails = self.dbs.listDatasetFileDetails(secondaryDataset, getParents=False, getLumis=needLumiInfo, validFileOnly=0)

                for secfilename, secinfos in moredetails.items():
                    secinfos['lumiobj'] = LumiList(runsAndLumis=secinfos['Lumis'])

                self.logger.info("Beginning to match files from secondary dataset")
                for dummyFilename, infos in filedetails.items():
                    infos['Parents'] = []
                    lumis = LumiList(runsAndLumis=infos['Lumis'])
                    for secfilename, secinfos in moredetails.items():
                        if lumis & secinfos['lumiobj']:
                            infos['Parents'].append(secfilename)
                self.logger.info("Done matching files from secondary dataset")
                kwargs['task']['tm_use_parent'] = 1
        except Exception as ex: #TODO should we catch HttpException instead?
            self.logger.exception(ex)
            raise TaskWorkerException("The CRAB3 server backend could not contact DBS to get the files details (Lumis, events, etc).\n"+\
                                "This is could be a temporary DBS glitch. Please try to submit a new task (resubmit will not work)"+\
                                " and contact the experts if the error persists.\nError reason: %s" % str(ex))
        if not filedetails:
            raise TaskWorkerException(("Cannot find any file inside the dataset. Please, check your dataset in DAS, %s.\n" +\
                                "Aborting submission. Resubmitting your task will not help.") %\
                                ("https://cmsweb.cern.ch/das/request?instance=%s&input=dataset=%s") %\
                                (self.dbsInstance, inputDataset))

        ## Format the output creating the data structures required by WMCore. Filters out invalid files,
        ## files whose block has no location, and figures out the PSN
        result = self.formatOutput(task=kwargs['task'], requestname=self.taskName,
                                   datasetfiles=filedetails, locations=locationsMap,
                                   tempDir=kwargs['tempDir'])

        if not result.result:
            raise TaskWorkerException(("Cannot find any valid file inside the dataset. Please, check your dataset in DAS, %s.\n" +
                                       "Aborting submission. Resubmitting your task will not help.") %
                                      ("https://cmsweb.cern.ch/das/request?instance=%s&input=dataset=%s") %
                                      (self.dbsInstance, inputDataset))

        self.logger.debug("Got %s files", len(result.result.getFiles()))

        return result

if __name__ == '__main__':
    ###
    # Usage: python3 DBSDataDiscovery.py dbs_instance dbsDataset [secondaryDataset]
    # where dbs_instance should be either prod/global or prod/phys03
    #
    # Example: python3 /data/repos/CRABServer/src/python/TaskWorker/Actions/DBSDataDiscovery.py prod/global /MuonEG/Run2016B-23Sep2016-v3/MINIAOD
    ###
    dbsInstance = sys.argv[1]
    dbsDataset = sys.argv[2]
    dbsSecondaryDataset = sys.argv[3] if len(sys.argv) == 4 else None
    DBSUrl = 'https://cmsweb.cern.ch/dbs/%s/DBSReader/' % dbsInstance

    logging.basicConfig(level=logging.DEBUG)
    from WMCore.Configuration import ConfigurationEx
    from ServerUtilities import newX509env

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

    config.Services.Rucio_host = 'https://cms-rucio.cern.ch'
    config.Services.Rucio_account = 'crab_server'
    config.Services.Rucio_authUrl = 'https://cms-rucio-auth.cern.ch'
    config.Services.Rucio_caPath = '/etc/grid-security/certificates/'
    rucioClient = getNativeRucioClient(config=config, logger=logging.getLogger())

    fileset = DBSDataDiscovery(config=config, rucioClient=rucioClient)
    fileset.execute(task={'tm_nonvalid_input_dataset': 'T', 'tm_use_parent': 0, 'user_proxy': 'None',
                          'tm_input_dataset': dbsDataset, 'tm_secondary_input_dataset': dbsSecondaryDataset,
                          'tm_taskname': 'pippo1', 'tm_username':config.Services.Rucio_account,
                          'tm_split_algo' : 'automatic', 'tm_split_args' : {'runs':[], 'lumis':[]},
                          'tm_user_config':{'partialdataset':False},
                          'tm_dbs_url': DBSUrl}, tempDir='')

#===============================================================================
#    Some interesting datasets for testing
#    dataset = '/DoubleMuon/Run2018B-PromptReco-v2/AOD'       # on tape
#    dataset = '/DoubleMuon/Run2018B-02Apr2020-v1/NANOAOD'    # isNano
#    dataset = '/DoubleMuon/Run2018B-17Sep2018-v1/MINIAOD'    # parent of above NANOAOD (for secondaryDataset lookup)
#    dataset = '/MuonEG/Run2016B-07Aug17_ver2-v1/AOD'         # no Nano on disk (at least atm)
#    dataset = '/MuonEG/Run2016B-v1/RAW'                      # on tape
#    dataset = '/MuonEG/Run2016B-23Sep2016-v3/MINIAOD'        # no NANO on disk (MINIAOD should always be on disk)
#    dataset = '/GenericTTbar/belforte-Stefano-Test-bb695911428445ed11a1006c9940df69/USER' # USER dataset on prod/phys03
#===============================================================================
