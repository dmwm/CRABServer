"""
Transfer object to share information between action classes.
"""
import logging
import json
import os
import hashlib
import shutil

import ASO.Rucio.config as config # pylint: disable=consider-using-from-import
from ASO.Rucio.exception import RucioTransferException
from ASO.Rucio.utils import writePath, parseFileNameFromLFN, addSuffixToProcessedDataset


class Transfer:
    """
    Use Transfer object to store state of process in memory.
    It responsible for read info from file and set its attribute.
    This object will pass to Actions class as mutable input.
    """
    def __init__(self):
        self.logger = logging.getLogger('RucioTransfer.Transfer')

        # from rest info
        self.restHost = ''
        self.restDBInstance = ''
        self.restProxyFile = ''

        # Content of transfers.txt (list of dict)
        # Note that we load whole file into memory but continue process from
        # `lastTransferLine`.
        self.transferItems = []

        # from transfer info
        self.rucioUsername = ''
        self.rucioScope = ''
        self.taskname = ''
        self.destination = ''
        self.publishContainer = ''
        self.transferContainer = ''
        self.logsDataset = ''
        self.multiPubContainers = []

        # dynamically change throughout the scripts
        self.currentDataset = ''

        # map of destination_lfn to transferItems
        self.LFN2transferItemMap = None

        # Bookkeeping variable
        # All variable here should be `None` and get assiged in `read*` method
        # to make it fail (fast) when we forgot to add `read*` in readInfo.
        self.lastTransferLine = None
        self.containerRuleID = None
        self.publishRuleID = None
        self.multiPubRuleIDs = {}
        self.bookkeepingOKLocks = None
        self.bookkeepingBlockComplete = None
        self.LFN2PFNMap = None
        self.cleanedFiles = None

    def readInfo(self):
        """
        Read the information from input files using path from configuration.
        It needs to execute to following order because of dependency between
        method.
        """
        if os.path.exists('task_process/transfers') and config.args.purge_transfers_dir:
            shutil.rmtree('task_process/transfers', ignore_errors=False)
        # ensure task_process/transfers directory
        if not os.path.exists('task_process/transfers'):
            os.makedirs('task_process/transfers')
        # read into memory
        self.readLastTransferLine()
        self.readTransferItems()
        self.buildLFN2transferItemMap()
        self.readRESTInfo()
        self.readInfoFromTransferItems()
        self.buildMultiPubContainerNames()
        self.readContainerRuleID()
        self.readOKLocks()
        self.readBlockComplete()
        self.readLFN2PFNMap()
        self.readCleanedFiles()

    def readInfoFromRucio(self, rucioClient):
        """
        Like `readInfo` but read the information from Rucio.

        :param rucioClient: Rucio client
        :type rucioClient: rucio.client.client.Client
        """
        # Currently, we do not use this method.

    def readLastTransferLine(self):
        """
        Reading lastTransferLine from task_process/transfers/last_transfer.txt
        """
        if not config.args.force_last_line is None: # Need explicitly compare to None
            self.lastTransferLine = config.args.force_last_line
            return
        path = config.args.last_line_path
        try:
            with open(path, 'r', encoding='utf-8') as r:
                self.lastTransferLine = int(r.read())
        except FileNotFoundError:
            self.logger.info(f'{path} not found. Assume it is first time it run.')
            self.lastTransferLine = 0

    def updateLastTransferLine(self, line):
        """
        Update lastTransferLine to task_process/transfers/last_transfer.txt

        :param line: line number
        :type line: int
        """
        self.lastTransferLine = line
        path = config.args.last_line_path
        with writePath(path) as w:
            w.write(str(self.lastTransferLine))

    def readTransferItems(self):
        """
        Reading transferItems (whole files) from task_process/transfers.txt
        """
        path = config.args.transfers_txt_path
        try:
            with open(path, 'r', encoding='utf-8') as r:
                for line in r:
                    doc = json.loads(line)
                    # Manipulate transfers dicts when running integration test
                    if config.args.force_publishname:
                        doc = manipulateOutputDataset(doc, config.args.force_publishname)
                    self.transferItems.append(doc)
        except FileNotFoundError as ex:
            raise RucioTransferException(f'{path} does not exist. Probably no completed jobs in the task yet.') from ex
        if len(self.transferItems) == 0:
            raise RucioTransferException(f'{path} does not contain new entry.')

    def buildLFN2transferItemMap(self):
        """
        Create map from destination LFN to transferItem.
        Note that LFN2transferItemMap only point to latest `destination_lfn` in
        case job has been retry.
        """
        self.LFN2transferItemMap = {}
        for x in self.transferItems:
            self.LFN2transferItemMap[x['destination_lfn']] = x

    def readRESTInfo(self):
        """
        Reading REST info from from task_process/RestInfoForFileTransfers.json
        """
        path = config.args.rest_info_path
        try:
            with open(path, 'r', encoding='utf-8') as r:
                doc = json.loads(r.read())
                self.restHost = doc['host']
                self.restDBInstance = doc['dbInstance']
                self.restProxyFile = doc['proxyfile']
        except FileNotFoundError as ex:
            raise RucioTransferException(f'{path} does not exist. Probably no completed jobs in the task yet.') from ex

    def readInfoFromTransferItems(self):
        """
        Convert info from first transferItems to this object attribute.
        Need to execute readTransferItems before this method.
        """
        # Get publish container name from the file that need to publish.
        info = self.transferItems[0]
        for t in self.transferItems:
            if not t['outputdataset'].startswith('/FakeDataset'):
                info = t
                break

        if info['destination_lfn'].startswith('/store/group/rucio/'):
            # Getting group's username from LFN path.
            name = info['destination_lfn'].split('/')[4]
            # Rucio group's username always has `_group` suffix.
            self.rucioUsername = f'{name}_group'
            self.rucioScope = f'group.{name}'
        else:
            self.rucioUsername = info['username']
            self.rucioScope = f'user.{self.rucioUsername}'
        self.taskname = info['taskname']
        self.destination = info['destination']
        containerName = info["outputdataset"]
        self.publishContainer = containerName
        tmp = containerName.split('/')
        taskNameHash = hashlib.md5(info['taskname'].encode()).hexdigest()[:8]
        tmp[2] += f'_TRANSFER.{taskNameHash}'
        self.transferContainer = '/'.join(tmp)
        self.logger.info(f'Publish container: {self.publishContainer}, Transfer container: {self.transferContainer}')
        self.logsDataset = f'{self.transferContainer}#LOGS'

    def buildMultiPubContainerNames(self):
        """
        Create the `self.multiPubContainers` by reading all transfers
        dict from the first job id available in transfer dicts.

        If it starts with '/FakeDatset', append filename to ProcessedName
        section of DBS dataset name.
        Otherwise, use it as is.

        Note that this method does not check the limit of the new container name
        length, but only relies on validation from REST.
        """
        multiPubContainers = []
        jobID = self.transferItems[0]['job_id']
        for item in self.transferItems:
            if item['job_id'] != jobID:
                break
            if item['outputdataset'].startswith('/FakeDataset'):
                filename = parseFileNameFromLFN(item['destination_lfn'])
                # Alter Rucio container name from `/FakeDataset/fakefile/USER`
                # To `/FakeDataset/fakefile_<filename>/USER`
                # E.g., `/FakeDataset/fakefile_myoutput.root/USER`
                containerName = addSuffixToProcessedDataset(item['outputdataset'], f'_{filename}')
            else:
                containerName = item['outputdataset']
            multiPubContainers.append(containerName)
        self.multiPubContainers = multiPubContainers

    def readContainerRuleID(self):
        """
        Read containerRuleID from task_process/transfers/container_ruleid.txt
        """
        # Skip reading rule from bookkeeping in case to intend to create
        # different container name when test.
        if config.args.force_publishname:
            return
        path = config.args.container_ruleid_path
        try:
            with open(path, 'r', encoding='utf-8') as r:
                tmp = json.load(r)
                self.containerRuleID = tmp['containerRuleID']
                self.publishRuleID = tmp['publishRuleID']
                self.multiPubRuleIDs = tmp['multiPubRuleIDs']
                self.logger.info('Got container rule ID from bookkeeping:')
                self.logger.info(f'  Transfer Container rule ID: {self.containerRuleID}')
                self.logger.info(f'  Publish Container rule ID: {self.publishRuleID}')
                self.logger.info(f'  Multiple Publish Container rule IDs: {self.multiPubRuleIDs}')
        except FileNotFoundError:
            self.containerRuleID = ''
            self.logger.info(f'Bookkeeping rules "{path}" does not exist. Assume it is first time it run.')

    def updateContainerRuleID(self):
        """
        update containerRuleID to task_process/transfers/container_ruleid.txt
        """
        path = config.args.container_ruleid_path
        self.logger.info(f'Bookkeeping Transfer Container rule ID [{self.containerRuleID}] and Pransfer Container and [{self.publishRuleID}] to file: {path}')
        with writePath(path) as w:
            json.dump({
                'containerRuleID': self.containerRuleID,
                'publishRuleID': self.publishRuleID,
                'multiPubRuleIDs': self.multiPubRuleIDs,
            }, w)

    def readOKLocks(self):
        """
        Read bookkeepingOKLocks from task_process/transfers/transfers_ok.txt.
        Initialize empty list in case of path not found or
        `--ignore-transfer-ok` is `True`.
        """
        if config.args.ignore_transfer_ok:
            self.bookkeepingOKLocks = []
            return
        path = config.args.transfer_ok_path
        try:
            with open(path, 'r', encoding='utf-8') as r:
                self.bookkeepingOKLocks = r.read().splitlines()
                self.logger.info(f'Got list of "OK" locks from bookkeeping: {self.bookkeepingOKLocks}')
        except FileNotFoundError:
            self.bookkeepingOKLocks = []
            self.logger.info(f'Bookkeeping path "{path}" does not exist. Assume this is first time it run.')

    def updateOKLocks(self, newLocks):
        """
        update bookkeepingOKLocks to task_process/transfers/transfers_ok.txt

        :param newLocks: list of LFN
        :type newLocks: list of string
        """
        self.bookkeepingOKLocks += newLocks
        path = config.args.transfer_ok_path
        self.logger.info(f'Bookkeeping transfer status OK: {self.bookkeepingOKLocks}')
        self.logger.info(f'to file: {path}')
        with writePath(path) as w:
            for l in self.bookkeepingOKLocks:
                w.write(f'{l}\n')

    def readBlockComplete(self):
        """
        Read bookkeepingBlockComplete from task_process/transfers/transfers_ok.txt.
        Initialize empty list in case of path not found or
        `--ignore-transfer-ok` is `True`.
        """
        if config.args.ignore_bookkeeping_block_complete:
            self.bookkeepingBlockComplete = []
            return
        path = config.args.bookkeeping_block_complete_path
        try:
            with open(path, 'r', encoding='utf-8') as r:
                self.bookkeepingBlockComplete = r.read().splitlines()
                self.logger.info(f'Got list of block complete from bookkeeping: {self.bookkeepingBlockComplete}')
        except FileNotFoundError:
            self.bookkeepingBlockComplete = []
            self.logger.info(f'Bookkeeping block complete path "{path}" does not exist. Assume this is first time it run.')

    def updateBlockComplete(self, newBlocks):
        """
        update bookkeepingBlockComplete to task_process/transfers/transfers_ok.txt

        :param newBlocks: list of LFN
        :type newBlocks: list of string
        """
        self.bookkeepingBlockComplete += newBlocks
        path = config.args.bookkeeping_block_complete_path
        self.logger.info (f'Bookkeeping block complete to file: {path}')
        self.logger.debug(f'{self.bookkeepingBlockComplete}')
        with writePath(path) as w:
            for l in self.bookkeepingBlockComplete:
                w.write(f'{l}\n')


    def populateLFN2DatasetMap(self, container, rucioClient):
        """
        Get the list of replicas in the container and return as key-value pair
        in `self.replicasInContainer` as a map of LFN to the dataset name it
        attaches to.

        :param container: container name
        :type: str
        :param rucioClient: Rucio client object
        :type rucioClient: rucio.client.client.Client
        :returns:
        :rtype:
        """
        replicasInContainer = {}
        datasets = rucioClient.list_content(self.rucioScope, container)
        for ds in datasets:
            files = rucioClient.list_content(self.rucioScope, ds['name'])
            for f in files:
                replicasInContainer[f['name']] = ds['name']
        return replicasInContainer

    def readLFN2PFNMap(self):
        """
        Read LFN2PFNMap from task_process/transfers/lfn2pfn_map.json
        Initialize empty dict in case of path not found or
        `--ignore-lfn2pfn-map` is `True`.
        """
        if config.args.ignore_lfn2pfn_map:
            self.LFN2PFNMap = {}
            return
        path = config.args.lfn2pfn_map_path
        try:
            with open(path, 'r', encoding='utf-8') as r:
                self.LFN2PFNMap = json.load(r)
                self.logger.info(f'Got LFN2PFN Map from bookkeeping: {self.LFN2PFNMap}')
        except FileNotFoundError:
            self.LFN2PFNMap = {}
            self.logger.info(f'Bookkeeping LFN2PFNMap file "{path}" does not exist. Assume this is first time it run.')

    def updateLFN2PFNMap(self):
        """
        update LFN2PFNMap to task_process/transfers/lfn2pfn_map.json
        Note that we did not check if dict in self.LFN2PFNMap are conform with
        format we expected.
        """
        path = config.args.lfn2pfn_map_path
        self.logger.info (f'Bookkeeping LFN2PFNMap to file: {path}')
        self.logger.debug(f'LFN2PFNMap: {self.LFN2PFNMap}')
        with writePath(path) as w:
            json.dump(self.LFN2PFNMap, w)

    def readCleanedFiles(self):
        """
        Read `self.cleanedFiles` from task_process/transfers/cleaned_files.json
        Initialize empty dict in case of path not found or
        `--ignore-cleanup-files` is `True`.
        """
        if config.args.ignore_cleaned_files:
            self.cleanedFiles = []
            return
        path = config.args.cleaned_files_path
        try:
            with open(path, 'r', encoding='utf-8') as r:
                self.cleanedFiles = json.load(r)
                self.logger.info(f'Got `cleanedFiles` from bookkeeping: {self.cleanedFiles}')
        except FileNotFoundError:
            self.cleanedFiles = []
            self.logger.info(f'`cleanedFiles` "{path}" does not exist. Assume this is first time it run.')

    def updateCleanedFiles(self):
        """
        update LFN2PFNMap to task_process/transfers/lfn2pfn_map.json
        Note that we did not check if dict in self.LFN2PFNMap are conform with
        format we expected.
        """
        path = config.args.cleaned_files_path
        self.logger.info (f'Bookkeeping `self.cleanedFiles` to file: {path}')
        self.logger.debug(f'self.cleanedFiles: {self.cleanedFiles}')
        with writePath(path) as w:
            json.dump(self.cleanedFiles, w)


def manipulateOutputDataset(transfer, forcePubName):
    """
    Replace 'outpudataset' key of transfer dicts to the new name.
    If /FakeDataset, use `forcePubName` to compute hash and append to the name.
    Else `forcePubName` is used.

    This helper function is used only to run integration tests, never to run
    in a normal task submission.
    """
    # import here to prevent import error in prod code.
    import copy # pylint: disable=import-outside-toplevel
    from ASO.Rucio.utils import addSuffixToProcessedDataset # pylint: disable=redefined-outer-name, reimported, import-outside-toplevel
    newTransfer = copy.deepcopy(transfer)
    newhash = hashlib.md5(forcePubName.encode()).hexdigest()[:8]
    if transfer['outputdataset'].startswith('/FakeDataset'):
        newTransfer['outputdataset'] = addSuffixToProcessedDataset(newTransfer['outputdataset'], f'.int{newhash}')
    else:
        newTransfer['outputdataset'] = forcePubName
    return newTransfer
