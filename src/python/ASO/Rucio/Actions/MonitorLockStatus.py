"""
Monitoring replicas' transfer status and adding them to publish the container.
"""
import logging
import copy
import datetime

import ASO.Rucio.config as config # pylint: disable=consider-using-from-import
from ASO.Rucio.utils import updateToREST, parseFileNameFromLFN
from ASO.Rucio.Actions.RegisterReplicas import RegisterReplicas
from ASO.Rucio.exception import RucioTransferException


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
        okFileDocs, notOKFileDocs = self.checkLockStatus()
        self.logger.debug(f'okFileDocs: {okFileDocs}')
        self.logger.debug(f'notOKFileDocs: {notOKFileDocs}')
        # skip locks that already update status to rest
        newDoneFileDocs = [doc for doc in okFileDocs if not doc['name'] in self.transfer.bookkeepingOKLocks]
        self.updateRESTFileDocsStateToDone(newDoneFileDocs)
        self.transfer.updateOKLocks([x['name'] for x in newDoneFileDocs])

        # NOTE: See https://github.com/dmwm/CRABServer/issues/7940
        ## Filter only files need to publish
        #needToPublishFileDocs = self.filterFilesNeedToPublish(okFileDocs)
        #self.logger.debug(f'needToPublishFileDocs: {needToPublishFileDocs}')
        ## Register transfer complete replicas to publish container.
        ## Note that replicas that already add to publish container will do
        ## nothing but return fileDoc containing the block name replica belongs
        ## to.
        #publishedFileDocs = self.registerToPublishContainer(needToPublishFileDocs)
        #publishedFileDocs = self.registerToPublishContainer(okFileDocs)
        publishedFileDocs = self.registerToMutiPubContainers(okFileDocs)
        self.logger.debug(f'publishedFileDocs: {publishedFileDocs}')
        # skip filedocs that already update it status to rest.
        newPublishFileDocs = [doc for doc in publishedFileDocs if not doc['dataset'] in self.transfer.bookkeepingBlockComplete]
        blockCompleteFileDocs = self.checkBlockCompleteStatus(newPublishFileDocs)
        # update block complete status
        self.logger.debug(f'fileDocs to update block completion: {blockCompleteFileDocs}')
        self.updateRESTFileDocsBlockCompletionInfo(blockCompleteFileDocs)
        # Bookkeeping published replicas (only replicas with blockcomplete "ok")
        newBlockComplete = list({doc['dataset'] for doc in blockCompleteFileDocs})
        self.transfer.updateBlockComplete(newBlockComplete)

    def checkLockStatus(self):
        """
        Check all lock status of replicas in the rule from
        `Transfer.containerRuleID`.

        :return: list of `okFileDocs` where locks is `OK` and `notOKFileDocs`
            where locks is `REPLICATING` or `STUCK`. Return as fileDoc format
        :rtype: tuple of list of dict
        """
        okFileDocs = []
        notOKFileDocs = []
        try:
            listReplicasLocks = self.rucioClient.list_replica_locks(self.transfer.containerRuleID)
        except TypeError:
            # Current rucio-clients==1.29.10 will raise exception when it get
            # None from server. It happen when we run list_replica_locks
            # immediately after register replicas in transfer container which
            # replicas lock info is not available yet.
            self.logger.info('Error was raised. Assume there is still no lock info available yet.')
            listReplicasLocks = []
        for lock in listReplicasLocks:
            fileDoc = {
                'id': self.transfer.LFN2transferItemMap[lock['name']]['id'],
                'name': lock['name'],
                'dataset': None,
                'blockcomplete': 'NO',
                'ruleid': self.transfer.containerRuleID,
            }
            if lock['state'] == 'OK':
                okFileDocs.append(fileDoc)
            else:
                notOKFileDocs.append(fileDoc)
        return (okFileDocs, notOKFileDocs)

    def registerToPublishContainer(self, fileDocs):
        """
        (Deprecated) Register replicas to the publish container. Update the replicas info
        to the new dataset name.

        :param fileDocs: replicas info return from `checkLockStatus` method.
        :type fileDocs: list of dict (fileDoc)

        :return: replicas info with updated dataset name.
        :rtype: list of dict
        """
        r = RegisterReplicas(self.transfer, self.rucioClient, None)
        publishContainerFileDocs = r.addReplicasToContainer(fileDocs, self.transfer.publishContainer)
        # Update dataset name for each replicas
        tmpLFN2DatasetMap = {x['name']:x['dataset'] for x in publishContainerFileDocs}
        tmpFileDocs = copy.deepcopy(fileDocs)
        for f in tmpFileDocs:
            f['dataset'] = tmpLFN2DatasetMap[f['name']]
        return tmpFileDocs

    def registerToMutiPubContainers(self, fileDocs):
        """
        Register replicas to it own publish container.

        In example, we have
        - output dataset name `/GenericTTbar/cmsbot-integration-1/USER`
        - 2 output files:
          - `output.root` (EDM)
          - `myfile.txt` (Misc)
        All `output_{job_id}.root` will registering to `/GenericTTbar/cmsbot-integration-1__output.root/USER`.
        All `myfile_{job_id}.txt` will registering to `/GenericTTbar/cmsbot-integration-1__myfile.txt/USER`.

        :param fileDocs: replicas info return from `checkLockStatus` method.
        :type fileDocs: list of dict (fileDoc)

        :return: replicas info with updated dataset name.
        :rtype: list of dict
        """
        r = RegisterReplicas(self.transfer, self.rucioClient, None)
        publishContainerFileDocs = []
        groupFileDocs = {}
        # Group by filename
        for fileDoc in fileDocs:
            filename = parseFileNameFromLFN(fileDoc['name'])
            if filename in groupFileDocs:
                groupFileDocs[filename].append(fileDoc)
            else:
                groupFileDocs[filename] = [fileDoc]
        # Register to its own publish container
        for filename, fileDocsInGroup in groupFileDocs.items():
            container = ''
            for c in self.transfer.multiPubContainers:
                tmp = c.split('/')[2].rsplit('__', 1)[1]
                if tmp == filename:
                    container = c
                    break
            if not container:
                raise RucioTransferException(f'Cannot find container for file: {filename} . There is a bug in the code.')
            # Now fileDoc dict is consist for the rest of Rucio ASO code.
            # We can return value from `addReplicasToContainer()` method.
            publishContainerFileDocs += r.addReplicasToContainer(fileDocsInGroup, container)
        return publishContainerFileDocs

    def checkBlockCompleteStatus(self, fileDocs):
        """
        Checking (DBS) block completion status from `is_open` dataset metadata.
        Only return list of fileDocs where `is_open` of the dataset is `False`.

        :param fileDocs: replicas's fileDocs
            method.
        :type fileDocs: list of dict

        :return: list of replicas info with updated `blockcomplete` to `OK`.
        :rtype: list of dict
        """
        tmpFileDocs = []
        datasetsMap = {}
        for i in fileDocs:
            datasetName = i['dataset']
            if not datasetName in datasetsMap:
                datasetsMap[datasetName] = [i]
            else:
                datasetsMap[datasetName].append(i)
        for dataset, v in datasetsMap.items():
            metadata = self.rucioClient.get_metadata(self.transfer.rucioScope, dataset)
            # TODO: Also close dataset when (in or)
            # - the task has completed.
            #   - We have no indication when task is completed unless it pass as
            #     args manually from caller scripts (task_proc_wrapper.sh)
            # - no new replica/is_open for more than 6 hours. DONE
            shouldClose = (metadata['updated_at'] + \
                           datetime.timedelta(seconds=config.args.open_dataset_timeout)) \
                           < datetime.datetime.now()
            if not metadata['is_open']:
                for f in v:
                    newF = copy.deepcopy(f)
                    newF['blockcomplete'] = 'OK'
                    tmpFileDocs.append(newF)
            elif shouldClose:
                self.logger.info(f'Closing dataset: {dataset}')
                self.rucioClient.close(self.transfer.rucioScope, dataset)
                for f in v:
                    newF = copy.deepcopy(f)
                    newF['blockcomplete'] = 'OK'
                    tmpFileDocs.append(newF)
            else:
                self.logger.info(f'Dataset {dataset} is still open.')
        return tmpFileDocs

    def filterFilesNeedToPublish(self, fileDocs):
        """
        Return only fileDoc that need to publish by publisher.
        Use the fact that non-edm files and logfiles will have '/FakeDataset'
        as outputdataset.

        :param fileDocs: list of fileDoc
        :type fileDocs: list of dict

        :return: fileDocs
        :rtype: list of dict
        """
        tmpPublishFileDocs = []
        for doc in fileDocs:
            transferItem = self.transfer.LFN2transferItemMap[doc['name']]
            if not transferItem['outputdataset'].startswith('/FakeDataset'):
                tmpPublishFileDocs.append(doc)
        return tmpPublishFileDocs

    def updateRESTFileDocsStateToDone(self, fileDocs):
        """
        Update files transfer state in filetransfersdb to DONE, along with
        metadata info required by REST.

        :param fileDocs: transferItems's ID and its information.
        :type fileDocs: list of dict
        """
        # TODO: may need to refactor later along with rest and publisher part
        num = len(fileDocs)
        restFileDoc = {
            'asoworker': 'rucio',
            'list_of_ids': [x['id'] for x in fileDocs],
            'list_of_transfer_state': ['DONE']*num,
            'list_of_dbs_blockname': None, # omit
            'list_of_block_complete': None, # omit
            'list_of_fts_instance': ['https://fts3-cms.cern.ch:8446/']*num,
            'list_of_failure_reason': None, # omit
            'list_of_retry_value': None, # omit
            'list_of_fts_id': [x['ruleid'] for x in fileDocs],
        }
        updateToREST(self.crabRESTClient, 'filetransfers', 'updateTransfers', restFileDoc)

    def updateRESTFileDocsBlockCompletionInfo(self, fileDocs):
        """
        Update block complete status of fileDocs to REST server.

        :param fileDocs: transferItems's ID and its information.
        :type fileDocs: list of dict
        """
        # TODO: may need to refactor later along with rest and publisher part
        # This can be optimize to single REST API call together with updateTransfers's subresources
        num = len(fileDocs)
        restFileDoc = {
            'asoworker': 'rucio',
            'list_of_ids': [x['id'] for x in fileDocs],
            'list_of_transfer_state': ['DONE']*num,
            'list_of_dbs_blockname': [x['dataset'] for x in fileDocs],
            'list_of_block_complete': [x['blockcomplete'] for x in fileDocs],
            'list_of_fts_instance': ['https://fts3-cms.cern.ch:8446/']*num,
            'list_of_failure_reason': None, # omit
            'list_of_retry_value': None, # omit
            'list_of_fts_id': None,
        }
        updateToREST(self.crabRESTClient, 'filetransfers', 'updateRucioInfo', restFileDoc)
