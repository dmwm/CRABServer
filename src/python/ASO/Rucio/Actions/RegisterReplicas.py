import logging
import itertools
import copy
from ASO.Rucio.Actions.BuildDBSDataset import BuildDBSDataset
from rucio.rse.rsemanager import find_matching_scheme
from rucio.common.exception import FileAlreadyExists

import ASO.Rucio.config as config
from ASO.Rucio.exception import RucioTransferException
from ASO.Rucio.utils import chunks, uploadToTransfersdb, tfcLFN2PFN, LFNToPFNFromPFN



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
        preparedReplicasByRSE = self.prepare(transferGenerator)
        # Add file to rucio by RSE
        successReplicas = self.addFilesToRucio(preparedReplicasByRSE)
        self.logger.debug(f'successReplicas: {successReplicas}')
        # Add replicas to transfer container
        self.addReplicasToContainer(successReplicas, self.transfer.transferContainer)
        # Create new entry in REST in FILETRANSFERDB table
        self.uploadTransferInfoToREST(successReplicas)
        # After everything is done, bookkeeping LastTransferLine.
        self.transfer.updateLastTransferLine(end)

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

        :returns: map of `<site>_Temp` and list of dicts that replicas information.
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
        for rse in bucket:
            xdict = bucket[rse][0]
            # We determine PFN of Temp RSE from normal RSE.
            # Simply remove temp suffix before passing to getSourcePFN function.
            pfn = self.getSourcePFN(xdict["source_lfn"], rse.split('_Temp')[0], xdict["destination"])
            replicasByRSE[rse] = []
            for xdict in bucket[rse]:
                replica = {
                    'scope': self.transfer.rucioScope,
                    'pfn': LFNToPFNFromPFN(xdict["source_lfn"], pfn),
                    'name': xdict['destination_lfn'],
                    'bytes': xdict['filesize'],
                    'adler32': xdict['checksums']['adler32'].rjust(8, '0'),
                    # TODO: move id out of replicas info
                    'id': xdict['id'],
                }
                replicasByRSE[rse].append(replica)
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
                try:
                    # TODO: remove id from dict we construct in prepare() method.
                    # remove 'id' from dict
                    r = []
                    for c in chunk:
                        d = c.copy()
                        d.pop('id')
                        r.append(d)
                    # add_replicas with same dids will always return True, even
                    # with changing metadata (e.g pfn), rucio will not update to
                    # the new value.
                    # See https://github.com/dmwm/CMSRucio/issues/343#issuecomment-1543663323
                    self.rucioClient.add_replicas(rse, r)
                except Exception as ex:
                    # Note that 2 exceptions we encounter so far here is due to
                    # LFN to PFN converstion and RSE protocols.
                    # https://github.com/dmwm/CRABServer/issues/7632
                    self.logger.error(f'add_replicas(rse, r): rse={rse} r={r}')
                    raise RucioTransferException('Something wrong with adding new replicas') from ex
                success = [{'id': x['id'], 'name': x['name']} for x in chunk]
                ret += success
        return ret

    def addReplicasToContainer(self, replicas, container):
        """
        Add `replicas` to the dataset in `container` in chunks (chunk size is
        defined in `config.args.replicas_chunk_size`). This including creates a
        new dataset when the current dataset exceeds
        `config.arg.max_file_per_datset`.

        :param replicas: a list contain dict with REST xfer id and replicas LFN.
        :type replicas: list
        """

        containerReplicas = []
        newReplicas = []
        # filter out duplicated replicas
        replicasInContainer = self.transfer.replicasInContainer[container]
        for r in replicas:
            if r['name'] in replicasInContainer:
                c = r.copy()
                c['dataset'] = replicasInContainer[r['name']]
                containerReplicas.append(c)
            else:
                newReplicas.append(r)

        b = BuildDBSDataset(self.transfer, self.rucioClient)
        currentDataset = b.getOrCreateDataset(container)
        self.logger.debug(f'currentDataset: {currentDataset}')
        for chunk in chunks(newReplicas, config.args.replicas_chunk_size):
            dids = [{
                'scope': self.transfer.rucioScope,
                'type': "FILE",
                'name': x["name"]
            } for x in chunk]
            # no need to try catch for duplicate content. Not sure if
            # restart process is enough for the case of connection error
            attachments = [{
                'scope': self.transfer.rucioScope,
                'name': currentDataset,
                'dids': dids
            }]
            self.rucioClient.add_files_to_datasets(attachments, ignore_duplicate=True)
            successItems = [{
                'id': x['id'],
                'name': x['name'],
                'dataset': currentDataset,
            } for x in chunk]
            containerReplicas += successItems
            # Current algo will add files whole chunk, so total number of
            # files in dataset is at most is max_file_per_datset+replicas_chunk_size.
            #
            # check the current number of files in the dataset
            num = len(list(self.rucioClient.list_content(self.transfer.rucioScope, currentDataset)))
            if num >= config.args.max_file_per_dataset:
                # FIXME: close the last dataset when ALL Postjob has reach timeout.
                self.rucioClient.close(self.transfer.rucioScope, currentDataset)
                currentDataset = b.getOrCreateDataset(container)
        return containerReplicas


    def getSourcePFN(self, sourceLFN, sourceRSE, destinationRSE):
        """
        Get source PFN from `rucioClient.lfns2pfns()`.

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
            if sourceRSE == 'T2_DE_DESY_Temp':
                pfn = pfn.replace('/pnfs/desy.de/cms/tier2/temp', '/pnfs/desy.de/cms/tier2/store/temp')
            # hardcode fix for T2_UK_SGrid_Bristol
            if sourceRSE == 'T2_UK_SGrid_Bristol_Temp':
                proto = self.rucioClient.get_protocols('T2_UK_SGrid_Bristol_Temp')[0]
                if proto['scheme'] != 'root':
                    raise RucioTransferException('Expected protocol scheme "root" from T2_UK_SGrid_Bristol_Temp (Temporary hardcoded).')
                pfn = f'{proto["scheme"]}://{proto["hostname"]}{proto["prefix"]}{"/".join(sourceLFN.split("/")[3:])}'
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

    def uploadTransferInfoToREST(self, replicas):
        """
        Upload transfers info to REST server.

        :param replicas: list of dict contains transferItems's ID and its
            information.
        :type replicas: list
        """
        num = len(replicas)
        fileDoc = {
            'asoworker': 'rucio',
            'list_of_ids': [x['id'] for x in replicas],
            'list_of_transfer_state': ['SUBMITTED']*num,
            'list_of_dbs_blockname': [x['dataset'] for x in replicas],
            'list_of_block_complete': ['NO']*num,
            'list_of_fts_instance': ['https://fts3-cms.cern.ch:8446/']*num,
            'list_of_failure_reason': None, # omit
            'list_of_retry_value': None, # omit
            'list_of_fts_id': ['NA']*num,
        }
        uploadToTransfersdb(self.crabRESTClient, 'filetransfers', 'updateTransfers', fileDoc, self.logger)
