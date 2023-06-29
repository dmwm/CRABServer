import logging
import copy

import ASO.Rucio.config as config
from ASO.Rucio.utils import uploadToTransfersdb
from ASO.Rucio.Actions.RegisterReplicas import RegisterReplicas

class MonitorLockStatus:
    """
    This class checks the status of the replicas by giving Rucio's rule ID and
    registering transfer completed replicas (status 'OK') to publish container,
    then updating the transfer status back to REST to notify PostJob.
    """
    def __init__(self, transfer, rucioClient, crabRESTClient):
        self.logger = logging.getLogger("RucioTransfer.Actions.MonitorLockStatus")
        self.rucioClient = rucioClient
        self.transfer = transfer
        self.crabRESTClient = crabRESTClient

    def execute(self):
        """
        Main execution step.
        """
        # Check replicas's lock status
        okReplicas, notOKReplicas = self.checkLockStatus()
        self.logger.debug(f'okReplicas: {okReplicas}')
        self.logger.debug(f'notOKReplicas: {notOKReplicas}')

        # Register transfer complete replicas to publish container.
        publishedReplicas = self.registerToPublishContainer(okReplicas)
        self.logger.debug(f'publishReplicas: {publishedReplicas}')
        # update ok status
        self.updateOKReplicasToREST(publishedReplicas)

        # update block complete status
        replicasToUpdateBlockComplete = self.checkBlockCompleteStatus(publishedReplicas)
        self.logger.debug(f'Replicas to update block completion: {replicasToUpdateBlockComplete}')
        self.updateBlockCompleteToREST(replicasToUpdateBlockComplete)

        # Bookkeeping published replicas (only replicas with blockcomplete "ok")
        self.transfer.updateTransferOKReplicas([x['name'] for x in replicasToUpdateBlockComplete])

    def checkLockStatus(self):
        """
        Check all lock status of replicas in the rule from
        `Transfer.containerRuleID`.

        :return: list of `okReplicas` where transfers are completed, and
            `notOkReplicas` where transfer transfers are still in REPLICATING
             or STUCK state. Each item in list is dict of replica info.
        :rtype: tuple of list of dict
        """
        okReplicas = []
        notOKReplicas = []
        try:
            listReplicasLocks = self.rucioClient.list_replica_locks(self.transfer.containerRuleID)
        except TypeError:
            # Current rucio-clients==1.29.10 will raise exception when it get
            # None response from server. It will happen when we run
            # list_replica_locks immediately after register replicas with
            # replicas lock info is not available yet.
            self.logger.info('Error has raised. Assume there is still no lock info available yet.')
            listReplicasLocks = []
        replicasInContainer = self.transfer.replicasInContainer[self.transfer.transferContainer]
        for replicaStatus in listReplicasLocks:
            # Skip replicas that register in the same run.
            if not replicaStatus['name'] in replicasInContainer:
                continue
            # skip if replicas transfer is in transferOKReplicas. No need to
            # update status for transfer complete.
            if replicaStatus['name'] in self.transfer.transferOKReplicas:
                continue

            replica = {
                'id': self.transfer.replicaLFN2IDMap[replicaStatus['name']],
                'name': replicaStatus['name'],
                'dataset': None,
                'blockcomplete': 'NO',
                'ruleid': self.transfer.containerRuleID,
            }
            if replicaStatus['state'] == 'OK':
                okReplicas.append(replica)
            else:
                notOKReplicas.append(replica)
        return (okReplicas, notOKReplicas)

    def registerToPublishContainer(self, replicas):
        """
        Register replicas to the published container. Update the replicas info
        to the new dataset name.

        :param replicas: replicas info return from `checkLockStatus` method.
        :type replicas: list of dict

        :return: replicas info with updated dataset name.
        :rtype: list of dict
        """
        r = RegisterReplicas(self.transfer, self.rucioClient, None)
        replicasPublishedInfo = r.addReplicasToContainer(replicas, self.transfer.publishContainer)
        # Update dataset name for each replicas
        tmpLFN2DatasetMap = {x['name']:x['dataset'] for x in replicasPublishedInfo}
        tmpReplicas = copy.deepcopy(replicas)
        for i in tmpReplicas:
            i['dataset'] = tmpLFN2DatasetMap[i['name']]
        return tmpReplicas

    def checkBlockCompleteStatus(self, replicas):
        """
        check (DBS) block completion status from `is_open` dataset metadata.
        Only return list of replica info where `is_open` of the dataset is
        `False`.

        :param replicas: replica info return from `registerToPublishContainer`
            method.
        :type replicas: list of dict

        :return: list of replicas info with updated `blockcomplete` to `OK`.
        :rtype: list of dict
        """
        tmpReplicas = []
        datasetsMap = {}
        for i in replicas:
            dataset = i['dataset']
            if not dataset in datasetsMap:
                datasetsMap[dataset] = [i]
            else:
                datasetsMap[dataset].append(i)
        for k, v in datasetsMap.items():
            metadata = self.rucioClient.get_metadata(self.transfer.rucioScope, k)
            # TODO: Also close dataset when (in or)
            # - the task has completed.
            # - no new replica/is_open for more than 6 hours
            if not metadata['is_open']:
                for r in v:
                    item = copy.copy(r)
                    item['blockcomplete'] = 'OK'
                    tmpReplicas.append(item)

        return tmpReplicas

    def updateOKReplicasToREST(self, replicas):
        """
        Update OK status transfers info to REST server.

        :param replicas: transferItems's ID and its information.
        :type replicas: list of dict
        """
        # TODO: may need to refactor later along with rest and publisher part
        num = len(replicas)
        fileDoc = {
            'asoworker': 'rucio',
            'list_of_ids': [x['id'] for x in replicas],
            'list_of_transfer_state': ['DONE']*num,
            'list_of_dbs_blockname': [x['dataset'] for x in replicas],
            'list_of_block_complete': [x['blockcomplete'] for x in replicas],
            'list_of_fts_instance': ['https://fts3-cms.cern.ch:8446/']*num,
            'list_of_failure_reason': None, # omit
            'list_of_retry_value': None, # omit
            'list_of_fts_id': ['NA']*num,
        }
        uploadToTransfersdb(self.crabRESTClient, 'filetransfers', 'updateTransfers', fileDoc, self.logger)

    def updateBlockCompleteToREST(self, replicas):
        """
        Update block complete status of replicas to REST server.

        :param replicas: transferItems's ID and its information.
        :type replicas: list of dict
        """
        # TODO: may need to refactor later along with rest and publisher part
        # This can be optimize to single REST API call together with updateTransfers's subresources
        num = len(replicas)
        fileDoc = {
            'asoworker': 'rucio',
            'list_of_ids': [x['id'] for x in replicas],
            'list_of_transfer_state': ['DONE']*num,
            'list_of_dbs_blockname': [x['dataset'] for x in replicas],
            'list_of_block_complete': [x['blockcomplete'] for x in replicas],
            'list_of_fts_instance': ['https://fts3-cms.cern.ch:8446/']*num,
            'list_of_failure_reason': None, # omit
            'list_of_retry_value': None, # omit
            'list_of_fts_id': ['NA']*num,
        }
        uploadToTransfersdb(self.crabRESTClient, 'filetransfers', 'updateRucioInfo', fileDoc, self.logger)
