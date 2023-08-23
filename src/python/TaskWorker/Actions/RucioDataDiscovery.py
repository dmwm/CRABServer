from __future__ import print_function

import copy
import logging
import os
import sys
from http.client import HTTPException

from RucioUtils import getNativeRucioClient
from TaskWorker.Actions.DataDiscovery import DataDiscovery
from TaskWorker.WorkerExceptions import TaskWorkerException


class RucioDataDiscovery(DataDiscovery):
    """Performing the data discovery through CMS Rucio service.
    """

    # disable pylint warning in next line since they refer to conflict with the main()
    # at the bottom of this file which is only used for testing
    def __init__(self, config, crabserver='', procnum=-1, rucioClient=None): # pylint: disable=redefined-outer-name
        DataDiscovery.__init__(self, config, crabserver, procnum)
        self.rucioClient = rucioClient

    def execute(self, *args, **kwargs):
        """
        This is a convenience wrapper around the executeInternal function
        """

        # we don't want to leak X509 credentials to other modules
        # so use a context manager to set an ad hoc env and restore as soon as
        # executeInternal is over, even if it raises exception

        with self.config.TaskWorker.envForCMSWEB:
            result = self.executeInternal(*args, **kwargs)

        return result

    def executeInternal(self, *args, **kwargs):

        self.logger.info("Data discovery with Rucio") ## to be changed into debug

        self.taskName = kwargs['task']['tm_taskname']           # pylint: disable=W0201
        self.username = kwargs['task']['tm_username']           # pylint: disable=W0201
        self.userproxy = kwargs['task']['user_proxy']           # pylint: disable=W0201

        if kwargs['task']['tm_split_algo'] != 'FileBased':
            msg = "Data.splitting must be set to 'FileBased' when using Rucio for DataDiscovery."
            raise TaskWorkerException(msg)
        if kwargs['task']['tm_use_parent']:
            msg = "Parent processing needs lumi info from DBS.\nCan't work when using Rucio for DataDiscovery."
            raise TaskWorkerException(msg)
        if kwargs['task']['tm_secondary_input_dataset']:
            msg = "Secondary dataset processing needs lumi info from DBS.\nCan't work when using Rucio for DataDiscovery."
            raise TaskWorkerException(msg)

        rucioDID = kwargs['task']['tm_input_dataset']
        rucioContainer = rucioDID.split(':')[1]
        rucioScope = rucioDID.split(':')[0]

        # when using Rucio for data discovery there is no lumi info and no secondary dataset
        # and it makes no difference USER dataset or not, the only thing that matter
        # is the correct scope for the given DBS dataset name i.e. Rucio container

        try:
            # Get the list of blocks for the locations.
            datasets = self.rucioClient.list_content(rucioScope, rucioContainer)
            blocks = [ds['name'] for ds in datasets]
        except Exception as ex:
            # dataset not found is a known use case
            if str(ex).find('No Dids found'):
                raise TaskWorkerException(f"Did not find container {rucioContainer} in scope {rucioScope}:") from ex
            raise
        if not blocks:
            raise TaskWorkerException(f"Rucio DID {rucioScope}:{rucioContainer} not existing or empty")

        ## Create a map for block's locations: for each block get the list of locations.
        ## locationsMap is a dictionary, key=blockName, value=list of PhedexNodes, example:
        ## {'/JetHT/Run2016B-PromptReco-v2/AOD#b10179dc-3723-11e6-9aa5-001e67abf228': [u'T1_IT_CNAF_Buffer', u'T2_US_Wisconsin', u'T1_IT_CNAF_MSS', u'T2_BE_UCL'],
        ## '/JetHT/Run2016B-PromptReco-v2/AOD#89b03ca6-1dc9-11e6-b567-001e67ac06a0': [u'T1_IT_CNAF_Buffer', u'T2_US_Wisconsin', u'T1_IT_CNAF_MSS', u'T2_BE_UCL']}


        # TODO move to a utility function common with DBSDataDiscovery
        # something like
        # (locationsMap, dataSize) = self.locateData(blocks, rucioScope)

        self.logger.info(f"Looking up data location with Rucio in {rucioScope}: scope.")
        locationsMap = {}
        totalSizeBytes = 0
        try:
            for blockName in blocks:
                replicas = set()
                response = self.rucioClient.list_dataset_replicas(scope=rucioScope, name=blockName, deep=True)
                for item in response:
                    if 'Tape' in item['rse']:
                        continue  # skip tape locations
                    if 'T3_CH_CERN_OpenData' in item['rse']:
                        continue  # ignore OpenData until it is accessible by CRAB
                    if item['state'].upper() == 'AVAILABLE':  # means all files in the block are on disk
                        replicas.add(item['rse'])
                        sizeBytes = item['bytes']
                if replicas:  # only fill map for blocks which have at least one location
                    locationsMap[blockName] = replicas
                    totalSizeBytes += sizeBytes
        except Exception as e:
            msg = f"Rucio lookup failed with\n{str(e)}"
            self.logger.warning(msg)
            locationsMap = None

        self.logger.debug(f"Dataset size in GBytes: {totalSizeBytes/1e9}")

        if not locationsMap:
            self.logger.warning(f"No locations found with Rucio for {rucioDID}")
            raise TaskWorkerException(
                "CRAB server could not get data locations from Rucio.\n" +
                "This is could be a temporary Rucio glitch, please try to submit a new task (resubmit will not work)" + \
                " and contact the experts if the error persists."
                )

        blocksWithLocation = locationsMap.keys()
        skipped = len(blocksWithLocation) != len(blocks)
        if skipped:
            msg = f"{skipped} blocks will not be processed because they have no (or not complete) disk replica"
            self.uploadWarning(msg, kwargs['task']['user_proxy'], kwargs['task']['tm_taskname'])

        try:
            # Here's DBS3Reader.listDatsetFileDetails re-implemented via Rucio API
            # DBSDataDiscovery has:
            # filedetails = self.dbs.listDatasetFileDetails(inputDataset, getParents=True, getLumis=needLumiInfo, validFileOnly=0)
            # filedetails is {lfn1:detail1, lfn2:detail2,...}
            # where for each file we have something like (from comments in DBS3Reader.py)
            #             { 'NumberOfEvents': 545,
            #               'BlockName': '/HighPileUp/Run2011A-v1/RAW#dd6e0796-cbcc-11e0-80a9-003048caaace',
            #               'Lumis': {173658: [8, 12, 9, 14, 19, 109, 105]},
            #               'Parents': [],
            #               'Checksum': '22218315',
            #               'Adler32': 'a41a1446',
            #               'Md5' : '23415' , # not listed in DBS3Reaser comments, but rquired in WMCore pff...
            #               'FileSize': 286021145,
            #               'ValidFile': 1
            #             }
            # since RucioDataDiscovery only supports FileBased splitting, we can
            # safely put dummy values for lumis and events, also there's no parentage in Rucio
            filedetails = {}
            detail = {  # a template
                # to be filled
                'BlockName': 'to be filled', 'Adler32': 'anAdler', 'FileSize': 0,
                # to maintain compatibility with downstream code that deals with data from DBS as well
                'ValidFile': 1, 'NumberOfEvents': 0, 'Lumis': {}, 'Parents': [],
                'Checksum': 'aChecksum', 'Md5': 'anMd5'}
            for dataset in blocksWithLocation:  #  DBS block = Rucio dataset
                detail['BlockName'] = dataset
                dids = self.rucioClient.list_files(rucioScope, dataset)
                for did in dids:
                    lfn = did['name']
                    detail['FileSize'] = did['bytes']
                    detail['Adler32'] = did['adler32']
                    filedetails[lfn] = copy.deepcopy(detail)
        except Exception as ex: #TODO should we catch HttpException instead?
            self.logger.exception(ex)
            raise TaskWorkerException("CRAB could not contact Rucio to get file list.\n"+\
                                "This could be a temporary glitch. Try to submit a new task (resubmit will not work)"+\
                                " and contact the experts if the error persists.\nError reason: %s" % str(ex))
        if not filedetails:
            raise TaskWorkerException("Cannot find any file inside the dataset. Check dataset scope and name\n" +\
                                "Aborting submission. Submitting your task again will not help.")

        ## Format the output creating the data structures required by WMCore. Filters out invalid files,
        ## files whose block has no location, and figures out the PSN
        result = self.formatOutput(task=kwargs['task'], requestname=self.taskName,
                                   datasetfiles=filedetails, locations=locationsMap,
                                   tempDir=kwargs['tempDir'])

        if not result.result:
            # the following message is wrong. failure inside formatOutput can fail for varied reasons
            msg = "Cannot find any valid file inside the input container."
            msg += f"\nPlease check {rucioDID}"
            msg += "\nAborting submission. Resubmitting your task will not help."
            raise TaskWorkerException(msg)

        self.logger.debug("Got %s files", len(result.result.getFiles()))

        return result

if __name__ == '__main__':
    ###
    # Usage: python3 RucioDataDiscovery.py rucioScope dbsDataset
    #
    # Example: python3 /data/repos/CRABServer/src/python/TaskWorker/Actions/RucioDataDiscovery.py cms /MuonEG/Run2016B-23Sep2016-v3/MINIAOD
    ###
    scope = sys.argv[1]
    container = sys.argv[2]
    did = f"{scope}:{container}"

    logging.basicConfig(level=logging.DEBUG)
    from ServerUtilities import newX509env
    from WMCore.Configuration import ConfigurationEx

    config = ConfigurationEx()
    config.section_("Services")
    config.section_("TaskWorker")

    # will user service cert as defined for TW
    if "X509_USER_CERT" in os.environ:
        config.TaskWorker.cmscert = os.environ["X509_USER_CERT"]
    else:
        config.TaskWorker.cmscert = '/data/certs/servicecert.pem'
    if "X509_USER_KEY" in os.environ:
        config.TaskWorker.cmskey = os.environ["X509_USER_KEY"]
    else:
        config.TaskWorker.cmskey = '/data/certs/servicekey.pem'

    config.TaskWorker.envForCMSWEB = newX509env(X509_USER_CERT=config.TaskWorker.cmscert,
                                                X509_USER_KEY=config.TaskWorker.cmskey)

    config.TaskWorker.instance = 'prod'

    config.Services.Rucio_host = 'https://cms-rucio.cern.ch'
    config.Services.Rucio_account = 'crab_server'
    config.Services.Rucio_authUrl = 'https://cms-rucio-auth.cern.ch'
    config.Services.Rucio_caPath = '/etc/grid-security/certificates/'
    rucioClient = getNativeRucioClient(config=config, logger=logging.getLogger())

    fileset = RucioDataDiscovery(config=config, rucioClient=rucioClient)
    fileset.execute(task={'tm_nonvalid_input_dataset': 'T', 'tm_use_parent': 0, 'user_proxy': 'None',
                          'tm_input_dataset': did, 'tm_split_algo': 'FileBased',
                          'tm_taskname': 'pippo1', 'tm_username': config.Services.Rucio_account,
                          }, tempDir='')

#===============================================================================
#    Some interesting datasets for testing
#    dataset = '/DoubleMuon/Run2018B-PromptReco-v2/AOD'       # on tape
#    dataset = '/DoubleMuon/Run2018B-02Apr2020-v1/NANOAOD'    # isNano
#    dataset = '/DoubleMuon/Run2018B-17Sep2018-v1/MINIAOD'    # parent of above NANOAOD (for secondaryDataset lookup)
#    dataset = '/MuonEG/Run2016B-07Aug17_ver2-v1/AOD'         # no Nano on disk (at least atm)
#    dataset = '/MuonEG/Run2016B-v1/RAW'                      # on tape
#    dataset = '/MuonEG/Run2016B-23Sep2016-v3/MINIAOD'        # no NANO on disk (MINIAOD should always be on disk)
#    dataset = '/GenericTTbar/belforte-Stefano-Test-bb695911428445ed11a1006c9940df69/USER' # USER dataset on prod/phys03
#===============================================================================
