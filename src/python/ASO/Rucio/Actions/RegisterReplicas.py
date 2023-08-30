"""
Registering files to Rucio.
"""

import logging
import itertools
import copy
from rucio.rse.rsemanager import find_matching_scheme

import ASO.Rucio.config as config # pylint: disable=consider-using-from-import
from ASO.Rucio.Actions.BuildDBSDataset import BuildDBSDataset
from ASO.Rucio.exception import RucioTransferException
from ASO.Rucio.utils import chunks, updateToREST, tfcLFN2PFN, LFNToPFNFromPFN


class RegisterReplicas:
    """
    RegisterReplicas action is responsible for registering new files in the temp
    area to Rucio. The transferring is done by Rucio's side (by the rule we
    created in BuildDBSDataset).
    """
    def __init__(self, transfer, rucioClient, crabRESTClient):
        self.logger = logging.getLogger("RucioTransfer.Actions.RegisterReplicas")
        self.rucioClient = rucioClient
        self.transfer = transfer
        self.crabRESTClient = crabRESTClient

    def execute(self):
        """
        Main execution steps to register replicas to datasets.
        """
        # Generate generator for range of transferItems we want to register.
        # This make it easier for do testing.
        start = self.transfer.lastTransferLine
        if config.args.force_total_files:
            end = start + config.args.force_total_files
        else:
            end = len(self.transfer.transferItems)
        transferGenerator = itertools.islice(self.transfer.transferItems, start, end)

        # Prepare
        transferItemsWithoutLogfile = self.skipLogTransfers(transferGenerator)
        preparedReplicasByRSE = self.prepare(transferItemsWithoutLogfile)
        # Add file to rucio by RSE
        successFileDocs = self.addFilesToRucio(preparedReplicasByRSE)
        self.logger.debug(f'successFileDocs: {successFileDocs}')
        # Add replicas to transfer container
        transferFileDocs = self.addReplicasToContainer(successFileDocs, self.transfer.transferContainer)
        # Update state of files in REST in FILETRANSFERDB table
        self.updateRESTFileDocStateToSubmitted(transferFileDocs)
        # After everything is done, bookkeeping LastTransferLine.
        self.transfer.updateLastTransferLine(end)

    def skipLogTransfers(self, transfers):
        """
        Temporary solution for filter out logfiles from transferItems when task
        has `tm_save_logs=T`. Force status logfiles in filetransfersdb to "DONE"
        (required by PostJob).

        Until we proper implement logs transfers with rucio later.

        :param transfers: iterator of transfers dict
        :type transfers: list of dict

        :return: new transfers object where logfiles are removed.
        :rtype: list of dict
        """
        newTransfers = []
        logFileDocs = []
        for xdict in transfers:
            if xdict["type"] == 'log':
                self.logger.info(f'Skipping {xdict["source_lfn"]}. Logs file transfer is not implemented.')
                logFileDocs.append(xdict['id'])
            else:
                newTransfers.append(xdict)
        num = len(logFileDocs)
        restFileDoc = {
            'asoworker': 'rucio',
            'list_of_ids': logFileDocs,
            'list_of_transfer_state': ['DONE']*num,
            'list_of_dbs_blockname': None,
            'list_of_block_complete': None,
            'list_of_fts_instance': ['https://fts3-cms.cern.ch:8446/']*num,
            'list_of_failure_reason': None, # omit
            'list_of_retry_value': None, # omit
            'list_of_fts_id': ['NA']*num,
        }
        updateToREST(self.crabRESTClient, 'filetransfers', 'updateTransfers', restFileDoc)
        return newTransfers

    def prepare(self, transfers):
        """
        Convert a list of transfer items to a ready-to-use variable for
        `register()` method. It receives Generator of `Transfer.transferItems`
        and constructs dicts of replicas cotain information needed for
        `rucioClient.add_replicas` function, grouped by source sites.

        We still need to resolve PFN manually because Temp RSE is
        non-deterministic. We rely on `rucioClient.lfn2pfns()` to determine the
        PFN of Temp RSE from normal RSE (The RSE without `Temp` suffix).

        :param transfers: the iterable object which produce item of transfer.
        :type transfers: iterator

        :returns: map of `<site>_Temp` with nested map of replicas information.
        :rtype: dict
        """
        # create bucket RSE
        bucket = {}
        replicasByRSE = {}
        for xdict in transfers:
            # /store/temp are register as `<site>_Temp` in rucio
            rse = f'{xdict["source"]}_Temp'
            if not rse in bucket:
                bucket[rse] = []
            bucket[rse].append(xdict)
        for rse, xdictList in bucket.items():
            xdict = xdictList[0]
            # We determine PFN of Temp RSE from normal RSE.
            # Simply remove temp suffix before passing to getSourcePFN function.
            pfn = self.getSourcePFN(xdict["source_lfn"], rse.split('_Temp')[0], xdict["destination"])
            replicasByRSE[rse] = {}
            for xdict in xdictList:
                replica = {
                    xdict['id'] : {
                        'scope': self.transfer.rucioScope,
                        'pfn': LFNToPFNFromPFN(xdict["source_lfn"], pfn),
                        'name': xdict['destination_lfn'],
                        'bytes': xdict['filesize'],
                        'adler32': xdict['checksums']['adler32'].rjust(8, '0'),
                    }
                }
                replicasByRSE[rse].update(replica)
        return replicasByRSE

    def addFilesToRucio(self, prepareReplicas):
        """
        Register files in Temp RSE in chunks per RSE (chunk size is defined in
        `config.args.replicas_chunk_size`).

        :param prepareReplicas: dict return from `prepare()` method.
        :type prepareReplicas: dict

        :returns: a list contain dict with REST xfer id and replicas LFN.
        :rtype: list
        """

        ret = []
        self.logger.debug(f'Prepare replicas: {prepareReplicas}')
        for rse, replicas in prepareReplicas.items():
            self.logger.debug(f'Registering replicas from {rse}')
            self.logger.debug(f'Replicas: {replicas}')
            for chunk in chunks(replicas, config.args.replicas_chunk_size):
                # Wa: I should make code more clear instead write comment like this.
                # chunk is slice of list of dict.items() return by chunks's utils function, only when we passing dict in first args.
                rs = [v for _, v in chunk]
                try:
                    # add_replicas with same dids will always return True, even
                    # with changing metadata (e.g pfn), rucio will not update to
                    # the new value.
                    # See https://github.com/dmwm/CMSRucio/issues/343#issuecomment-1543663323
                    self.rucioClient.add_replicas(rse, rs)
                except Exception as ex:
                    # Note that 2 exceptions we encounter so far here is due to
                    # LFN to PFN converstion and RSE protocols.
                    # https://github.com/dmwm/CRABServer/issues/7632
                    self.logger.error(f'add_replicas(rse, r): rse={rse} rs={rs}')
                    raise RucioTransferException('Something wrong with adding new replicas') from ex
                for idx, r in chunk:
                    fileDoc = {
                        'id': idx,
                        'name': r['name'],
                        'dataset': None,
                        'blockcomplete': None,
                        'ruleid': None,
                    }
                    ret.append(fileDoc)
        return ret

    def addReplicasToContainer(self, fileDocs, container):
        """
        Add `replicas` to the dataset in `container` in chunks (chunk size is
        defined in `config.args.replicas_chunk_size`). This including creates a
        new dataset when the current dataset exceeds
        `config.arg.max_file_per_datset`.

        :param fileDocs: a list of fileDoc info an
        :type fileDocs: list of dict
        :return: same list as fileDocs args but with updated rule iD and dataset
            name
        :rtype: list of dict
        """

        containerFileDocs = []
        newFileDocs = []

        # filter out duplicated replicas
        replicasInContainer = self.transfer.populateLFN2DatasetMap(container, self.rucioClient)
        for r in fileDocs:
            if r['name'] in replicasInContainer:
                c = copy.deepcopy(r)
                c['dataset'] = replicasInContainer[r['name']]
                c['ruleid'] = self.transfer.containerRuleID
                containerFileDocs.append(c)
            else:
                newFileDocs.append(r)

        b = BuildDBSDataset(self.transfer, self.rucioClient, self.crabRESTClient)
        for chunk in chunks(newFileDocs, config.args.replicas_chunk_size):
            currentDataset = b.getOrCreateDataset(container)
            self.logger.debug(f'currentDataset: {currentDataset}')
            dids = [{
                'scope': self.transfer.rucioScope,
                'type': "FILE",
                'name': x["name"]
            } for x in chunk]
            # no need to try catch for duplicate content.
            attachments = [{
                'scope': self.transfer.rucioScope,
                'name': currentDataset,
                'dids': dids
            }]
            self.rucioClient.add_files_to_datasets(attachments, ignore_duplicate=True)
            for c in chunk:
                success = {
                    'id': c['id'],
                    'name': c['name'],
                    'dataset': currentDataset,
                    'blockcomplete': None,
                    'ruleid': self.transfer.containerRuleID,
                }
                containerFileDocs.append(success)

            # Current algo will add files whole chunk, so total number of
            # files in dataset is at most is max_file_per_datset+replicas_chunk_size.
            #
            # check the current number of files in the dataset
            num = len(list(self.rucioClient.list_content(self.transfer.rucioScope, currentDataset)))
            if num >= config.args.max_file_per_dataset:
                self.logger.info(f'Closing dataset: {currentDataset}')
                self.rucioClient.close(self.transfer.rucioScope, currentDataset)
        return containerFileDocs


    def getSourcePFN(self, sourceLFN, sourceRSE, destinationRSE):
        """
        Get source PFN from `rucioClient.lfns2pfns()`.

        The PFN stored in the Temp_RSE is not a general thing, but it is a
        PFN which can only be used for replicating this file to a specific
        destination. Given that, current code is clear enough, and forcing a
        'davs' scheme would not make it any easier to understand.

        All in all, we expect replicas in Temp_RSE to only stay there for one
        month max.

        :param sourceLFN: source LFN
        :type sourceLFN: string
        :param sourceRSE: source RSE where LFN is reside, but it must be normal
            RSE name (e.g. `T2_CH_CERN` without suffix `_Temp`). Otherwise, it
            will raise exception in `rucioClient.lfns2pfns()`.
        :type sourceRSE: string
        :param destinationRSE: need for select proper protocol for transfer
            with `find_matching_scheme()`.
        :type destinationRSE: string

        :returns: PFN return from `lfns2pfns()`
        :rtype: string
        """
        self.logger.debug(f'Getting pfn for {sourceLFN} at {sourceRSE}')
        try:
            _, srcScheme, _, _ = find_matching_scheme(
                {"protocols": self.rucioClient.get_protocols(destinationRSE)},
                {"protocols": self.rucioClient.get_protocols(sourceRSE)},
                "third_party_copy_read",
                "third_party_copy_write",
            )
            did = f'{self.transfer.rucioScope}:{sourceLFN}'
            sourcePFNMap = self.rucioClient.lfns2pfns(sourceRSE, [did], operation="third_party_copy_read", scheme=srcScheme)
            pfn = sourcePFNMap[did]
            # hardcode fix for DESY temp,
            if sourceRSE == 'T2_DE_DESY':
                pfn = pfn.replace('/pnfs/desy.de/cms/tier2/temp', '/pnfs/desy.de/cms/tier2/store/temp')
            self.logger.debug(f'PFN: {pfn}')
            return pfn
        except Exception as ex:
            raise RucioTransferException("Failed to get source PFN") from ex

    def getSourcePFN2(self, sourceLFN, sourceRSE):
        """
        Just for crosschecking with FTS algo we use in `getSourcePFN()`
        Will remove it later.
        """
        self.logger.debug(f'Getting pfn for {sourceLFN} at {sourceRSE}')
        rgx = self.rucioClient.get_protocols(
            sourceRSE, protocol_domain='ALL', operation="read")[0]
        didStr = f'{self.transfer.rucioScope}:{sourceLFN}'
        if not rgx['extended_attributes'] or 'tfc' not in rgx['extended_attributes']:
            pfn = self.rucioClient.lfns2pfns(
                sourceRSE, [didStr], operation="read")[didStr]
        else:
            tfc = rgx['extended_attributes']['tfc']
            tfc_proto = rgx['extended_attributes']['tfc_proto']
            pfn = tfcLFN2PFN(sourceLFN, tfc, tfc_proto)

        if sourceRSE == 'T2_DE_DESY':
            pfn = pfn.replace('/pnfs/desy.de/cms/tier2/temp', '/pnfs/desy.de/cms/tier2/store/temp')
        self.logger.debug(f'PFN2: {pfn}')
        return pfn

    def updateRESTFileDocStateToSubmitted(self, fileDocs):
        """
        Update files transfer state in filetransfersdb to SUBMITTED, along with
        metadata info required by REST.

        :param fileDocs: list of dict contains transferItems's ID and its
            information.
        :type fileDocs: list
        """
        num = len(fileDocs)
        restFileDoc = {
            'asoworker': 'rucio',
            'list_of_ids': [x['id'] for x in fileDocs],
            'list_of_transfer_state': ['SUBMITTED']*num,
            'list_of_dbs_blockname': None, # omit, will update it in MonitorLockStatus action
            'list_of_block_complete': None, # omit, will update it in MonitorLockStatus action
            'list_of_fts_instance': ['https://fts3-cms.cern.ch:8446/']*num,
            'list_of_failure_reason': None, # omit
            'list_of_retry_value': None, # omit
            'list_of_fts_id': [x['ruleid'] for x in fileDocs]
        }
        updateToREST(self.crabRESTClient, 'filetransfers', 'updateTransfers', restFileDoc)
