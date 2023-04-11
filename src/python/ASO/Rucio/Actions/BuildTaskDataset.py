import logging
import uuid

from rucio.common.exception import DataIdentifierAlreadyExists, InvalidObject, DuplicateRule, DuplicateContent

from ASO.Rucio.exception import RucioTransferException
from ASO.Rucio.config import config

class BuildTaskDataset():
    def __init__(self, transfer, rucioClient):
        self.logger = logging.getLogger("RucioTransfer.Actions.BuildTaskDataset")
        self.rucioClient = rucioClient
        self.transfer = transfer

    def execute(self):
        self.check_or_create_container()
        # create log dataset
        self.createDataset(self.transfer.logsDataset)
        self.transfer.currentDataset = self.getOrCreateDataset()

    def check_or_create_container(self):
        """
        creating container
         - check if container already exists
         - otherwise create it
        """
        try:
            self.rucioClient.add_container(self.transfer.rucioScope, self.transfer.publishname)
            self.logger.info(f"{self.transfer.publishname} container created")
        except DataIdentifierAlreadyExists:
            self.logger.info(f"{self.transfer.publishname} container already exists, doing nothing")
        except Exception as ex:
            raise RucioTransferException('Failed to create container') from ex

    def getOrCreateDataset(self):
        # Check if there are open datasets and then start from there:
        # - if open with less than max file go ahead and use it
        # - if max file is reached, close the currente ds and open a new one
        # - if force_create = True, create a new one anyway

        # TODO: create #LOGS dataset if does not exists
        # Can we simply avoid transferring LOGS with RUCIO?
        datasets = self.rucioClient.list_content(self.transfer.rucioScope, self.transfer.publishname)
        # remove log dataset
        datasets = [ds for ds in datasets if not ds['name'].endswith('#LOG')]
        # get_metadata_bulk "Always" raise InvalidObject.
        # probably a bug on rucio server, even production block.
        try:
            metadata = self.rucioClient.get_metadata_bulk(datasets)
        except InvalidObject:
            # Cover the case for which the dataset has been created but has 0 files
            metadata = []
            for ds in datasets:
                metadata.append(self.rucioClient.get_metadata(self.transfer.rucioScope, ds["name"]))
        openDatasets = [md['name'] for md in metadata if md['is_open'] == True]
        if len(openDatasets) == 0:
            self.logger.info("No dataset available yet, creating one")
            currentDatasetName = f'{self.transfer.publishname}#{str(uuid.uuid4())}'
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
        self.createDataset(currentDatasetName)
        return currentDatasetName

    def createDataset(self, datasetName):
        self.logger.debug(f'Creating dataset {datasetName}')
        try:
            self.rucioClient.add_dataset(self.transfer.rucioScope, datasetName)
        except DataIdentifierAlreadyExists:
            self.logger.info(f"{datasetName} dataset already exists, doing nothing")
        ds_did = {'scope': self.transfer.rucioScope, 'type': "DATASET", 'name': datasetName}
        try:
            self.rucioClient.add_replication_rule([ds_did], 1, self.transfer.destination)
            # TODO: not sure if any other case make the rule duplicate beside script crash
        except DuplicateRule:
            self.logger.info(f"Rule already exists, doing nothing")
        try:
        # attach dataset to the container
            self.rucioClient.attach_dids(self.transfer.rucioScope, self.transfer.publishname, [ds_did])
        except DuplicateContent as ex:
            self.logger.info(f'{datasetName} dataset has attached to {self.transfer.publishname}, doing nothing')
