import logging
import json
import os

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
        self.publishname = ''
        self.logsDataset = ''

        # dynamically change throughout the scripts
        self.currentDataset = ''

        # bookkeeping
        self.lastTransferLine = 0

        # rule bookkeeping
        self.containerRuleID = ''

        # all replicas from rucio
        self.replicasInContainer = None

        # map lf2 to id
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

    def readInfoFromRucio(self, rucioClient):
        """
        Read the information from Rucio.

        :param rucioClient: Rucio client
        :type rucioClient: rucio.client.client.Client
        """
        self.getReplicasInContainer(rucioClient)

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
        Create `self.replicaLFN2IDMap` for map from LFN to ID of REST file
        trasnfer.
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
            self.publishname = config.args.force_publishname
        else:
            self.publishname = info['outputdataset']
        self.logsDataset = f'{self.publishname}#LOGS'

    def readContainerRuleID(self):
        """
        Read containerRuleID from task_process/transfers/bookkeeping_rules.json
        """
        # skip reading rule from bookkeeping in case rerun with new publishname.
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
        update task_process/transfers/container_ruleid.txt
        """
        self.containerRuleID = ruleID
        path = config.args.container_ruleid_path
        self.logger.info(f'Bookkeeping container rule ID [{ruleID}] to file: {path}')
        with writePath(path) as w:
            w.write(ruleID)

    def getReplicasInContainer(self, rucioClient):
        """
        Get the list of replicas in the container and store it as key-value pair
        in `self.replicasInContainer`, as a map of LFN to the dataset name it
        attaches to.

        :param rucioClient: Rucio Client
        :type rucioClient: rucio.client.client.Client
        """
        replicasInContainer = {}
        datasets = rucioClient.list_content(self.rucioScope, self.publishname)
        for ds in datasets:
            files = rucioClient.list_content(self.rucioScope, ds['name'])
            for f in files:
                if not f['name'] in replicasInContainer:
                    replicasInContainer[f['name']] = ds['name']
        self.logger.debug(f'all replicas in container: {replicasInContainer}')
        self.replicasInContainer = replicasInContainer
