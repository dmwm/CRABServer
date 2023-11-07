"""
Create Rucio's container and dataset.
"""
import logging
import uuid
import json

from rucio.common.exception import DataIdentifierAlreadyExists, InvalidObject, DuplicateRule, DuplicateContent

from ASO.Rucio.exception import RucioTransferException
from ASO.Rucio.utils import updateToREST

class BuildDBSDataset():
    """
    Class to create Rucio's container and dataset.

    :param transfer: Transfer Object to get infomation.
    :type object: class:`ASO.Rucio.Transfer`
    :param rucioClient: Rucio Client object.
    :type object: class:`rucio.client.client.Client`
    """
    def __init__(self, transfer, rucioClient, crabRESTClient):
        self.logger = logging.getLogger("RucioTransfer.Actions.BuildDBSDataset")
        self.rucioClient = rucioClient
        self.transfer = transfer
        self.crabRESTClient = crabRESTClient

    def execute(self):
        """
        Construct DBS Dataset by creating a new Rucio container and adding a
        LOGS dataset.
        """
        # create container
        if not self.transfer.containerRuleID:
            self.transfer.containerRuleID = self.checkOrCreateContainer(self.transfer.transferContainer)
            self.transfer.publishRuleID = self.checkOrCreateContainer(self.transfer.publishContainer)
            # multi pub container
            for containerName  in self.transfer.multiPubContainers:
                ruleID = self.checkOrCreateContainer(containerName)
                self.transfer.multiPubRuleIDs[containerName] = ruleID
            # Upload rule id and transfer container name to TasksDB.
            # Note that we upload multiPubRuleIDs as json string to REST and
            # manually load back in client side later.
            configreq = {
                'workflow': self.transfer.taskname,
                'transfercontainer': self.transfer.transferContainer,
                'transferrule': self.transfer.containerRuleID,
                'publishrule': self.transfer.publishRuleID,
                'multipubrulejson': json.dumps(self.transfer.multiPubRuleIDs),
            }
            updateToREST(self.crabRESTClient, 'task', 'addrucioasoinfo', configreq)
            # bookkeeping
            self.transfer.updateContainerRuleID()

        # create log dataset in transfer container
        self.createDataset(self.transfer.transferContainer, self.transfer.logsDataset)

    def checkOrCreateContainer(self, container):
        """
        Creating container and attach replication rules. ignore if it already
        exists. Return rule ID to caller.

        :param container: container name
        :param type: str
        :returns: rucio rule id
        :rtype: str
        """
        self.logger.info(f'Creating container "{self.transfer.rucioScope}:{container}')
        try:
            self.rucioClient.add_container(self.transfer.rucioScope, container)
            self.logger.info(f"{container} container created")
        except DataIdentifierAlreadyExists:
            self.logger.info(f"{container} container already exists, doing nothing")
        except Exception as ex:
            raise RucioTransferException('Failed to create container') from ex

        self.logger.info(f'Add replication rule to container "{self.transfer.rucioScope}:{container}')
        try:
            containerDID = {
                'scope': self.transfer.rucioScope,
                'name': container,
                'type': "CONTAINER",
            }
            ruleID = self.rucioClient.add_replication_rule([containerDID], 1, self.transfer.destination)[0]
        except DuplicateRule:
            # TODO: it is possible that someone will create the rule for container, need better filter rule to match rules we create
            self.logger.info("Rule already exists. Get rule ID from Rucio.")
            ruleID = list(self.rucioClient.list_did_rules(self.transfer.rucioScope, container))[0]['id']
        self.logger.info(f"Got rule ID [{ruleID}] for {container}.")
        return ruleID


    def getOrCreateDataset(self, container):
        """
        Get or create new dataset.
        - If open more than 2, choose one and close other.
        - If only one is open, go ahead and use it.
        - if none, create new one.
        Note that this method always call createDataset() to ensure that datasets
        are attached to container.

        :param container: container name
        :param type: str
        :returns: dataset name
        :rtype: str
        """

        datasets = self.rucioClient.list_content(self.transfer.rucioScope, container)
        # remove log dataset
        datasets = [ds for ds in datasets if not ds['name'].endswith('#LOGS')]
        self.logger.debug(f"datasets in container: {datasets}")

        # get_metadata_bulk "Always" raise InvalidObject.
        # Probably a bug on rucio server, even production block.
        try:
            metadata = self.rucioClient.get_metadata_bulk(datasets)
        except InvalidObject:
            # Cover the case for which the dataset has been created but has 0 files
            metadata = []
            for ds in datasets:
                metadata.append(self.rucioClient.get_metadata(self.transfer.rucioScope, ds["name"]))
        openDatasets = [md['name'] for md in metadata if md['is_open']]
        self.logger.debug(f"open datasets: {datasets}")
        if len(openDatasets) == 0:
            self.logger.info("No dataset available yet, creating one")
            currentDatasetName = self.generateDatasetName(container)
        elif len(openDatasets) == 1:
            currentDatasetName = openDatasets[0]
            self.logger.info(f"Found exactly one open dataset: {currentDatasetName}")
        # which case we went through this?
        else:
            self.logger.info(
                "Found more than one open dataset, closing the one with more files and using the other as the current one")
            # TODO: close the most occupied and take the other as the current one -
            # so far we take the first and then let the Publisher close the dataset when task completed
            currentDatasetName = openDatasets[0]
        # always execute createDataset() again in case replication rule is not create and the dids is not attach to root container
        self.createDataset(container, currentDatasetName)
        return currentDatasetName

    def createDataset(self, container, dataset):
        """
        Creating Rucio dataset, add replication rule, attach to container.
        Ignore error if it already done.

        :param dataset: container name dataset need to attach to.
        :param type: str
        :param dataset: dataset name to create.
        :param type: str
        """
        self.logger.debug(f'Creating dataset {dataset}')
        try:
            self.rucioClient.add_dataset(self.transfer.rucioScope, dataset)
        except DataIdentifierAlreadyExists:
            self.logger.info(f"{dataset} dataset already exists, doing nothing")
        dsDID = {'scope': self.transfer.rucioScope, 'type': "DATASET", 'name': dataset}
        try:
            # attach dataset to the container
            self.rucioClient.attach_dids(self.transfer.rucioScope, container, [dsDID])
        except DuplicateContent:
            self.logger.info(f'{dataset} dataset has attached to {container}, doing nothing')

    @staticmethod
    def generateDatasetName(container):
        """
        Return a new dataset name.

        :returns: string of dataset name.
        """
        return f'{container}#{uuid.uuid4()}'
