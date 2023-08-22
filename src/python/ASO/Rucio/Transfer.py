"""
Transfer object to share information between action classes.
"""
import logging
import json
import os
import hashlib

import ASO.Rucio.config as config # pylint: disable=consider-using-from-import
from ASO.Rucio.exception import RucioTransferException
from ASO.Rucio.utils import writePath

class Transfer: # pylint: disable=too-many-instance-attributes
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

        # content of transfers.txt
        self.transferItems = []

        # from transfer info
        self.username = ''
        self.rucioScope = ''
        self.destination = ''
        self.publishContainer = ''
        self.transferContainer = ''
        self.logsDataset = ''

        # dynamically change throughout the scripts
        self.currentDataset = ''

        # bookkeeping
        self.lastTransferLine = 0
        self.containerRuleID = ''
        self.bookkeepingOKLocks = None
        self.bookkeepingBlockComplete = None

        # map of lfn to original info in transferItems
        self.LFN2transferItemMap = None

    def readInfo(self):
        """
        Read the information from input files using path from configuration.
        It needs to execute to following order because of dependency between
        method.
        """
        # ensure task_process/transfers directory
        if not os.path.exists('task_process/transfers'):
            os.makedirs('task_process/transfers')
        # read into memory
        self.readLastTransferLine()
        self.readTransferItems()
        self.buildLFN2transferItemMap()
        self.readRESTInfo()
        self.readInfoFromTransferItems()
        self.readContainerRuleID()
        self.readOKLocks()
        self.readBlockComplete()

    def readInfoFromRucio(self, rucioClient):
        """
        Read the information from Rucio.

        :param rucioClient: Rucio client
        :type rucioClient: rucio.client.client.Client
        """

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
                    self.transferItems.append(doc)
        except FileNotFoundError as ex:
            raise RucioTransferException(f'{path} does not exist. Probably no completed jobs in the task yet.') from ex
        if len(self.transferItems) == 0:
            raise RucioTransferException(f'{path} does not contain new entry.')

    def buildLFN2transferItemMap(self):
        """
        Create map from LFN to transferItem
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

        self.username = info['username']
        self.rucioScope = f'user.{self.username}'
        self.destination = info['destination']
        if config.args.force_publishname:
            containerName = config.args.force_publishname
        else:
            containerName = info["outputdataset"]
        self.publishContainer = containerName
        tmp = containerName.split('/')
        taskNameHash = hashlib.md5(info['taskname'].encode()).hexdigest()[:8]
        tmp[2] += f'_TRANSFER.{taskNameHash}'
        self.transferContainer = '/'.join(tmp)
        self.logger.info(f'Publish container: {self.publishContainer}, Transfer container: {self.transferContainer}')
        self.logsDataset = f'{self.transferContainer}#LOGS'

    def readContainerRuleID(self):
        """
        Read containerRuleID from task_process/transfers/container_ruleid.txt
        """
        # skip reading rule from bookkeeping in case rerun with new transferContainerName.
        if config.args.force_publishname:
            return
        path = config.args.container_ruleid_path
        try:
            with open(path, 'r', encoding='utf-8') as r:
                self.containerRuleID = r.read()
                self.logger.info(f'Got container rule ID from bookkeeping. Rule ID: {self.containerRuleID}')
        except FileNotFoundError:
            self.logger.info(f'Bookkeeping rules "{path}" does not exist. Assume it is first time it run.')

    def updateContainerRuleID(self, ruleID):
        """
        update containerRuleID to task_process/transfers/container_ruleid.txt
        """
        self.containerRuleID = ruleID
        path = config.args.container_ruleid_path
        self.logger.info(f'Bookkeeping container rule ID [{ruleID}] to file: {path}')
        with writePath(path) as w:
            w.write(ruleID)

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
