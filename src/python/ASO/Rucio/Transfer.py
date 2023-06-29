import logging
import json
import os
import hashlib

from ASO.Rucio.exception import RucioTransferException
from ASO.Rucio.utils import writePath
import ASO.Rucio.config as config

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
        self.transferOKReplicas = None

        # info from rucio
        self.replicasInContainer = None

        # map lfn to id
        self.replicaLFN2IDMap = None

        # info from rucio
        self.replicasInContainer = None

        # map lfn to id
        self.replicaLFN2IDMap = None

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
        self.buildLFN2IDMap()
        self.readRESTInfo()
        self.readInfoFromTransferItems()
        self.readContainerRuleID()
        self.readTransferOKReplicas()

    def readInfoFromRucio(self, rucioClient):
        """
        Read the information from Rucio.

        :param rucioClient: Rucio client
        :type rucioClient: rucio.client.client.Client
        """
        self.initReplicasInContainer(rucioClient)

    def readInfoFromRucio(self, rucioClient):
        """
        Read the information from Rucio.

        :param rucioClient: Rucio client
        :type rucioClient: rucio.client.client.Client
        """
        self.initReplicasInContainer(rucioClient)

    def readLastTransferLine(self):
        """
        Reading lastTransferLine from task_process/transfers/last_transfer.txt
        """
        if config.args.force_last_line != None: #  Need explicitly compare to None
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
        self.transferItems = []
        try:
            with open(path, 'r', encoding='utf-8') as r:
                for line in r:
                    doc = json.loads(line)
                    self.transferItems.append(doc)
        except FileNotFoundError as ex:
            raise RucioTransferException(f'{path} does not exist. Probably no completed jobs in the task yet.') from ex
        if len(self.transferItems) == 0:
            raise RucioTransferException(f'{path} does not contain new entry.')

    def buildLFN2IDMap(self):
        """
        Create `self.replicaLFN2IDMap` map from LFN to REST ID using using info
        in self.transferItems
        """
        self.replicaLFN2IDMap = {}
        for x in self.transferItems:
            self.replicaLFN2IDMap[x['destination_lfn']] = x['id']

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
        info = self.transferItems[0]
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

    def readTransferOKReplicas(self):
        """
        Read transferOKReplicas from task_process/transfers/transfers_ok.txt
        """
        if config.args.ignore_transfer_ok:
            self.transferOKReplicas = []
            return
        path = config.args.transfer_ok_path
        try:
            with open(path, 'r', encoding='utf-8') as r:
                self.transferOKReplicas = r.read().splitlines()
                self.logger.info(f'Got list of transfer status "OK" from bookkeeping: {self.transferOKReplicas}')
        except FileNotFoundError:
            self.transferOKReplicas = []
            self.logger.info(f'Bookkeeping transfer status OK from path "{path}" does not exist. Assume this is first time it run.')

    def updateTransferOKReplicas(self, newReplicas):
        """
        update transferOKReplicas to task_process/transfers/transfers_ok.txt

        :param newReplicas: list of LFN
        :type newReplicas: list of string
        """
        self.transferOKReplicas += newReplicas
        if config.args.ignore_transfer_ok:
            return
        path = config.args.transfer_ok_path
        self.logger.info(f'Bookkeeping transfer status OK: {self.transferOKReplicas}')
        self.logger.info(f'to file: {path}')
        with writePath(path) as w:
            for l in self.transferOKReplicas:
                w.write(f'{l}\n')

    def initReplicasInContainer(self, rucioClient):
        """
        Get replicas from transfer and publish container and assign it to
        self.replicasInContainer as key-value pare of containerName and map of
        LFN2Dataset.

        :param rucioClient: Rucio Client
        :type rucioClient: rucio.client.client.Client
        """
        replicasInContainer = {}
        replicasInContainer[self.transferContainer] = self.populateLFN2DatasetMap(self.transferContainer, rucioClient)
        replicasInContainer[self.publishContainer] = self.populateLFN2DatasetMap(self.publishContainer, rucioClient)
        self.replicasInContainer = replicasInContainer

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
