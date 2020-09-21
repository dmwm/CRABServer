from __future__ import print_function
import os
import sys
import logging
from httplib import HTTPException
import urllib

from WMCore.DataStructs.LumiList import LumiList
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.Rucio.Rucio import Rucio
from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.Services.DBS.DBSErrors import DBSReaderError

from TaskWorker.WorkerExceptions import TaskWorkerException, TapeDatasetException
from TaskWorker.Actions.DataDiscovery import DataDiscovery
from TaskWorker.Actions.DDMRequests import blocksRequest
from ServerUtilities import tempSetLogLevel

class DBSDataDiscovery(DataDiscovery):
    """Performing the data discovery through CMS DBS service.
    """

    def checkDatasetStatus(self, dataset, kwargs):
        res = self.dbs.dbs.listDatasets(dataset=dataset, detail=1, dataset_access_type='*')
        if not res:
            raise TaskWorkerException("Cannot find dataset %s in %s DBS instance" % (dataset, self.dbsInstance))
        if len(res) > 1:
            raise TaskWorkerException("Found more than one dataset while checking in DBS the status of %s" % dataset)
        res = res[0]
        #import pprint
        #self.logger.info("Input dataset details: %s", pprint.pformat(res))
        accessType = res['dataset_access_type']
        if accessType != 'VALID':
            # as per Dima's suggestion https://github.com/dmwm/CRABServer/issues/4739
            msgForDeprecDS = "Please contact your physics group if you think the dataset should not be deprecated."
            if kwargs['task']['tm_nonvalid_input_dataset'] != 'T':
                msg = "CRAB refuses to proceed in getting the details of the dataset %s from DBS, because the dataset is not 'VALID' but '%s'." % (dataset, accessType)
                if accessType == 'DEPRECATED':
                    msg += " (%s)" % (msgForDeprecDS)
                msg += " To allow CRAB to consider a dataset that is not 'VALID', set Data.allowNonValidInputDataset = True in the CRAB configuration."
                msg += " Notice that this will not force CRAB to run over all files in the dataset;"
                msg += " CRAB will still check if there are any valid files in the dataset and run only over those files."
                raise TaskWorkerException(msg)
            msg = "The input dataset %s is not 'VALID' but '%s'." % (dataset, accessType)
            msg += " CRAB will check if there are any valid files in the dataset and run only over those files."
            if accessType == 'DEPRECATED':
                msg += " %s" % (msgForDeprecDS)
            self.uploadWarning(msg, kwargs['task']['user_proxy'], kwargs['task']['tm_taskname'])
        return

    def keepOnlyDiskRSEs(self, locationsMap):
        # get all the RucioStorageElements (RSEs) which are of kind 'Disk'
        # locationsMap is a dictionary {block1:[locations], block2:[locations],...}

        diskLocationsMap = {}
        for block, locations in locationsMap.iteritems():
            diskRSEs = [rse for rse in locations if not 'Tape' in rse]  # as of Sept 2020, tape RSEs ends with _Tape
            if  'T3_CH_CERN_OpenData' in diskRSEs:
                diskRSEs.remove('T3_CH_CERN_OpenData') # ignore OpenData until it is accessible by CRAB
            if diskRSEs:
                # at least some locations are disk
                diskLocationsMap[block] = diskRSEs
            else:
                # no locations are disk, assume that they are tape
                # and keep tally of tape-only locations for this dataset
                self.tapeLocations = self.tapeLocations.union(set(locations) - set(diskRSEs))
        locationsMap.clear() # remove all blocks
        locationsMap.update(diskLocationsMap) # add only blocks with disk locations

    def keepOnlyDisks(self, locationsMap):
        phedex = PhEDEx() # TODO use certs from the config!
        # get all the PNNs that are of kind 'Disk'
        try:
            diskLocations = set([pnn['name'] for pnn in phedex.getNodeMap()['phedex']['node'] if pnn['kind'] == 'Disk'])
        except HTTPException as ex:
            self.logger.error(ex.headers)
            raise TaskWorkerException("The CRAB3 server backend could not contact phedex to get the list of site storages.\n"+\
                                "This is could be a temporary phedex glitch, please try to submit a new task (resubmit will not work)"+\
                                " and contact the experts if the error persists.\nError reason: %s" % str(ex)) # TODO addo the nodes phedex so the user can check themselves
        diskLocationsMap = {}
        for block, locations in locationsMap.iteritems():
            locations[:] = [x for x in locations if x != 'T3_CH_CERN_OpenData'] # ignore OpenData until it is accessible by CRAB
            if set(locations) & diskLocations:
                # at least some locations are disk
                diskLocationsMap[block] = locationsMap[block]
            else:
                # no locations are in the disk list, assume that they are tape
                self.tapeLocations = self.tapeLocations.union(set(locations) - diskLocations)
        locationsMap.clear() # remove all blocks
        locationsMap.update(diskLocationsMap) # add only blocks with disk locations

    def checkBlocksSize(self, blocks):
        """ Make sure no single blocks has more than 100k lumis. See
            https://hypernews.cern.ch/HyperNews/CMS/get/dmDevelopment/2022/1/1/1/1/1/1/2.html
        """
        MAX_LUMIS = 100000
        for block in blocks:
            blockInfo = self.dbs.getDBSSummaryInfo(block=block)
            if blockInfo.get('NumberOfLumis', 0) > MAX_LUMIS:
                msg = "Block %s contains more than %s lumis.\nThis blows up CRAB server memory" % (block, MAX_LUMIS)
                msg += "\nCRAB can only split this by ignoring lumi information. You can do this"
                msg += "\nusing FileBased split algorithm and avoiding any additional request"
                msg += "\nwich may cause lumi information to be looked up. See CRAB FAQ for more info:"
                msg += "\nhttps://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ"
                raise TaskWorkerException(msg)

    def execute(self, *args, **kwargs):
        """
        This is a convenience wrapper around the executeInternal function
        """

        # DBS3 requires X509_USER_CERT to be set - but we don't want to leak that to other modules
        # so use a context manager to set an ad hoc env and restore as soon as
        # executeInternal is over, even if it raises exception

        with self.config.TaskWorker.envForCMSWEB:
            result = self.executeInternal(*args, **kwargs)

        return result

    def executeInternal(self, *args, **kwargs):


        self.logger.info("Data discovery with DBS") ## to be changed into debug


        if kwargs['task']['tm_dbs_url']:
            dbsurl = kwargs['task']['tm_dbs_url']
        else:
            dbsurl = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'  # a sensible default
        if hasattr(self.config.Services, 'DBSHostName'):
            hostname = dbsurl.split('//')[1].split('/')[0]
            dbsurl = dbsurl.replace(hostname, self.config.Services.DBSHostName)
        self.logger.info("will connect to DBS at URL: %s", dbsurl)
        self.dbs = DBSReader(dbsurl)
        self.dbsInstance = self.dbs.dbs.serverinfo()["dbs_instance"]
        isUserDataset = self.dbsInstance.split('/')[1] != 'global'
        # where to look locations in pre-Rucio world
        PhEDExOrDBS = 'PhEDEx' if not isUserDataset else 'DBS origin site'

        taskName = kwargs['task']['tm_taskname']
        userProxy = kwargs['task']['user_proxy']
        self.logger.debug("Data discovery through %s for %s", self.dbs, taskName)

        inputDataset = kwargs['task']['tm_input_dataset']
        secondaryDataset = kwargs['task'].get('tm_secondary_input_dataset', None)

        self.checkDatasetStatus(inputDataset, kwargs)
        if secondaryDataset:
            self.checkDatasetStatus(secondaryDataset, kwargs)

        try:
            # Get the list of blocks for the locations.
            blocks = self.dbs.listFileBlocks(inputDataset)
            if secondaryDataset:
                secondaryBlocks = self.dbs.listFileBlocks(secondaryDataset)
        except DBSReaderError as dbsexc:
            # dataset not found in DBS is a known use case
            if str(dbsexc).find('No matching data'):
                raise TaskWorkerException("CRAB could not find dataset %s in this DBS instance: %s" % inputDataset, dbsurl)
            raise
        ## Create a map for block's locations: for each block get the list of locations.
        ## Note: listFileBlockLocation() gets first the locations from PhEDEx, and if no
        ## locations are found it gets the original locations from DBS. So it should
        ## never be the case at this point that some blocks have no locations.
        ## locationsMap is a dictionary, key=blockName, value=list of PhedexNodes, example:
        ## {'/JetHT/Run2016B-PromptReco-v2/AOD#b10179dc-3723-11e6-9aa5-001e67abf228': [u'T1_IT_CNAF_Buffer', u'T2_US_Wisconsin', u'T1_IT_CNAF_MSS', u'T2_BE_UCL'],
        ## '/JetHT/Run2016B-PromptReco-v2/AOD#89b03ca6-1dc9-11e6-b567-001e67ac06a0': [u'T1_IT_CNAF_Buffer', u'T2_US_Wisconsin', u'T1_IT_CNAF_MSS', u'T2_BE_UCL']}

        # For now apply Rucio data location only to NANOAOD*
        # in time useRucioForLocations may become a more rich expression
        isNano = blocks[0].split("#")[0].split("/")[-1] in ["NANOAOD", "NANOAODSIM"]
        if isNano:
            self.logger.info("NANOAOD* datset. Will use Rucio for data location")
        useRucioForLocations = not isUserDataset
        locationsFoundWithRucio = False

        if not useRucioForLocations:
            self.logger.info("Will not use Rucio for this dataset")
        # if locations should be in Rucio, try it first and fall back to old ways if Rucio calls fail
        # of if they return no locations (possible Rucio teething pain). If Rucio returns a list, trust it.
        if useRucioForLocations:
            locationsMap = {}
            scope = "cms"
            # If the dataset is a USER one, use the Rucio user scope to find it
            # TODO: we need a way to enable users to indicate others user scopes as source
            if isUserDataset:
                scope = "user.%s" % kwargs['task']['tm_username']
            rucio_config_dict = {
                "phedexCompatible": True,
                "auth_type": "x509", "ca_cert": self.config.Services.Rucio_caPath,
                "logger" : self.logger,
                "creds": {"client_cert": self.config.TaskWorker.cmscert, "client_key": self.config.TaskWorker.cmskey}
            }
            try:
                self.logger.info("Initializing Rucio client")
                # WMCore is awfully verbose
                with tempSetLogLevel(logger=self.logger, level=logging.ERROR):
                    rucioClient = Rucio(
                        self.config.Services.Rucio_account,
                        hostUrl=self.config.Services.Rucio_host,
                        authUrl=self.config.Services.Rucio_authUrl,
                        configDict=rucio_config_dict
                    )
                rucioClient.whoAmI()
                self.logger.info("Looking up data location with Rucio in %s scope.", scope)
                with tempSetLogLevel(logger=self.logger, level=logging.ERROR):
                    locations = rucioClient.getReplicaInfoForBlocks(scope=scope, block=list(blocks))
            except Exception as exc:
                msg = "Rucio lookup failed with\n%s" % str(exc)
                # TODO when removing fall-back to PhEDEx, this should be a fatal error
                # raise TaskWorkerException(msg)
                self.logger.warn(msg)
                locations = None

            # TODO when removing fall-back to PhEDEx, above code will raise if it fails, therefore
            # the following "if" must be removed and the code shifted left
            if locations:
                located_blocks = locations['phedex']['block']
                for block in located_blocks:
                    if block['replica']:  # only fill map for blocks which have at least one location
                        locationsMap.update({block['name']: [x['node'] for x in block['replica']]})
                if locationsMap:
                    locationsFoundWithRucio = True
                    # filter out TAPE locations
                    self.keepOnlyDiskRSEs(locationsMap)

                else:
                    msg = "No locations found with Rucio for this dataset"
                    # since NANO* are not in PhEDEx, this should be a fatal error
                    if isNano:
                        raise TaskWorkerException(msg)
                    else:
                        # note it down and try with PhEDEx
                        self.logger.warn(msg)

        if not locationsFoundWithRucio:  # fall back to pre-Rucio methods
            try:
                self.logger.info("No locations found with Rucio")
                self.logger.info("Looking up data locations using %s", PhEDExOrDBS)
                locationsMap = self.dbs.listFileBlockLocation(list(blocks), dbsOnly=isUserDataset)
            except Exception as ex:
                raise TaskWorkerException(
                    "The CRAB3 server backend could not get the location of the files from dbs nor phedex nor rucio.\n"+\
                    "This is could be a temporary phedex/rucio/dbs glitch, please try to submit a new task (resubmit will not work)"+\
                    " and contact the experts if the error persists.\nError reason: %s" % str(ex)
                    )
            # only fill map for blocks which have at least one location
            locationsMap = {key: value for key, value in locationsMap.iteritems() if value}
            self.keepOnlyDisks(locationsMap)

        if secondaryDataset:
            secondaryLocationsMap = {}
            # see https://github.com/dmwm/CRABServer/issues/6075#issuecomment-641569446
            self.logger.info("Trying data location of secondary blocks with Rucio")
            try:
                locations = rucioClient.getReplicaInfoForBlocks(scope=scope, block=list(secondaryBlocks))
            except Exception as exc:
                locations = None
                secondaryLocationsMap = {}
                self.logger.warn("Rucio lookup failed with. %s", exc)
            if locations:
                located_blocks = locations['phedex']['block']
                for block in located_blocks:
                    if block['replica']:   # only fill map for blocks which have at least one location
                        secondaryLocationsMap.update({block['name']: [x['node'] for x in block['replica']]})
            if not secondaryLocationsMap:
                msg = "No locations found with Rucio for secondaryDataset."
                # TODO when removing fall-back to PhEDEx, this should be a fatal error
                # raise TaskWorkerException(msg)
                self.logger.warn(msg)
                self.logger.info("Trying data location of secondary blocks with PhEDEx")
                try:
                    secondaryLocationsMap = self.dbs.listFileBlockLocation(list(secondaryBlocks), dbsOnly=isUserDataset)
                except Exception as ex:
                    raise TaskWorkerException(
                        "The CRAB3 server backend could not get the location of the secondary dataset files from dbs or phedex or rucio.\n" + \
                        "This is could be a temporary phedex/rucio/dbs glitch, please try to submit a new task (resubmit will not work)" + \
                        " and contact the experts if the error persists.\nError reason: %s" % str(ex)
                    )
                # only fill map for blocks which have at least one location
                secondaryLocationsMap = {key: value for key, value in secondaryLocationsMap.iteritems() if value}

        # From now on code is not dependent from having used Rucio or PhEDEx
        
        blocksWithLocation = locationsMap.keys()
        if secondaryDataset:
            secondaryBlocksWithLocation = secondaryLocationsMap.keys()

        if not locationsMap:
            msg = "Task could not be submitted because there is no DISK replica for dataset %s" % inputDataset
            if self.tapeLocations:
                msg += "\nN.B.: the input dataset is stored at %s, but those are TAPE locations." % ', '.join(sorted(self.tapeLocations))
                # submit request to DDM
                ddmRequest = None
                ddmServer = self.config.TaskWorker.DDMServer
                try:
                    ddmRequest = blocksRequest(blocksWithLocation, ddmServer, self.config.TaskWorker.cmscert, self.config.TaskWorker.cmskey, verbose=False)
                except HTTPException as hte:
                    self.logger.exception(hte)
                    msg += "\nThe automatic stage-out failed, please try again later. If the error persists contact the experts and provide this error message:"
                    msg += "\nHTTP Error while contacting the DDM server %s:\n%s" % (ddmServer, str(hte))
                    msg += "\nHTTP Headers are: %s" % hte.headers
                    msg += "\nYou might want to contact your physics group if you need a disk replica."
                    raise TaskWorkerException(msg, retry=True)

                self.logger.info("Contacted %s using %s and %s, got:\n%s", self.config.TaskWorker.DDMServer, self.config.TaskWorker.cmscert, self.config.TaskWorker.cmskey, ddmRequest)
                # The query above returns a JSON with a format {"result": "OK", "message": "Copy requested", "data": [{"request_id": 18, "site": <site>, "item": [<list of blocks>], "group": "AnalysisOps", "n": 1, "status": "new", "first_request": "2018-02-26 23:57:37", "last_request": "2018-02-26 23:57:37", "request_count": 1}]}
                if ddmRequest["result"] == "OK":
                    # set status to TAPERECALL
                    tapeRecallStatus = 'TAPERECALL'
                    ddmReqId = ddmRequest["data"][0]["request_id"]
                    configreq = {'workflow': taskName,
                                 'taskstatus': tapeRecallStatus,
                                 'ddmreqid': ddmReqId,
                                 'subresource': 'addddmreqid',
                                }
                    try:
                        tapeRecallStatusSet = self.server.post(self.restURInoAPI+'/task', data=urllib.urlencode(configreq))
                    except HTTPException as hte:
                        self.logger.exception(hte)
                        msg = "HTTP Error while contacting the REST Interface %s:\n%s" % (self.config.TaskWorker.restHost, str(hte))
                        msg += "\nSetting %s status and DDM request ID (%d) failed for task %s" % (tapeRecallStatus, ddmReqId, taskName)
                        msg += "\nHTTP Headers are: %s" % hte.headers
                        raise TaskWorkerException(msg, retry=True)

                    msg += "\nA disk replica has been requested on %s to CMS DDM (request ID: %d)" % (ddmRequest["data"][0]["first_request"], ddmReqId)
                    if tapeRecallStatusSet[2] == "OK":
                        self.logger.info("Status for task %s set to '%s'", taskName, tapeRecallStatus)
                        msg += "\nThis task will be automatically submitted as soon as the stage-out is completed."
                        self.uploadWarning(msg, userProxy, taskName)

                        raise TapeDatasetException(msg)
                    else:
                        msg += ", please try again in two days."

                else:
                    msg += "\nThe disk replica request failed with this error:\n %s" % ddmRequest["message"]

            msg += "\nPlease, check DAS (https://cmsweb.cern.ch/das) and make sure the dataset is accessible on DISK."
            raise TaskWorkerException(msg)

        # will not need lumi info if user has asked for split by file with no run/lumi mask
        splitAlgo = kwargs['task']['tm_split_algo']
        lumiMask = kwargs['task']['tm_split_args']['lumis']
        runRange = kwargs['task']['tm_split_args']['runs']

        needLumiInfo = splitAlgo != 'FileBased' or lumiMask != [] or runRange != []
        # secondary dataset access relies on run/lumi info
        if secondaryDataset:
            needLumiInfo = True
        if needLumiInfo:
            self.checkBlocksSize(blocksWithLocation) # Interested only in blocks with locations, 'blocks' may contain invalid ones and trigger an Exception
            if secondaryDataset:
                self.checkBlocksSize(secondaryBlocksWithLocation)
        try:
            filedetails = self.dbs.listDatasetFileDetails(inputDataset, getParents=True, getLumis=needLumiInfo, validFileOnly=0)
            if secondaryDataset:
                moredetails = self.dbs.listDatasetFileDetails(secondaryDataset, getParents=False, getLumis=needLumiInfo, validFileOnly=0)

                for secfilename, secinfos in moredetails.items():
                    secinfos['lumiobj'] = LumiList(runsAndLumis=secinfos['Lumis'])

                self.logger.info("Beginning to match files from secondary dataset")
                for dummyFilename, infos in filedetails.items():
                    infos['Parents'] = []
                    lumis = LumiList(runsAndLumis=infos['Lumis'])
                    for secfilename, secinfos in moredetails.items():
                        if lumis & secinfos['lumiobj']:
                            infos['Parents'].append(secfilename)
                self.logger.info("Done matching files from secondary dataset")
                kwargs['task']['tm_use_parent'] = 1
        except Exception as ex: #TODO should we catch HttpException instead?
            self.logger.exception(ex)
            raise TaskWorkerException("The CRAB3 server backend could not contact DBS to get the files details (Lumis, events, etc).\n"+\
                                "This is could be a temporary DBS glitch. Please try to submit a new task (resubmit will not work)"+\
                                " and contact the experts if the error persists.\nError reason: %s" % str(ex))
                                #TODO addo the nodes phedex so the user can check themselves
        if not filedetails:
            raise TaskWorkerException(("Cannot find any file inside the dataset. Please, check your dataset in DAS, %s.\n" +\
                                "Aborting submission. Resubmitting your task will not help.") %\
                                ("https://cmsweb.cern.ch/das/request?instance=%s&input=dataset=%s") %\
                                (self.dbsInstance, inputDataset))

        ## Format the output creating the data structures required by WMCore. Filters out invalid files,
        ## files whose block has no location, and figures out the PSN
        result = self.formatOutput(task=kwargs['task'], requestname=taskName,
                                   datasetfiles=filedetails, locations=locationsMap,
                                   tempDir=kwargs['tempDir'])

        if not result.result:
            raise TaskWorkerException(("Cannot find any valid file inside the dataset. Please, check your dataset in DAS, %s.\n" +
                                       "Aborting submission. Resubmitting your task will not help.") %
                                      ("https://cmsweb.cern.ch/das/request?instance=%s&input=dataset=%s") %
                                      (self.dbsInstance, inputDataset))

        self.logger.debug("Got %s files", len(result.result.getFiles()))

        return result

if __name__ == '__main__':
    """
    Usage: python DBSDataDiscovery.py dbs_instance dbsDataset [secondaryDataset]
    where dbs_instance should be either prod/global or prod/phys03
    Needs to define first:
    export X509_USER_CERT=/data/certs/servicecert.pem
    export X509_USER_KEY=/data/certs/servicekey.pem

    Example: python ~/repos/CRABServer/src/python/TaskWorker/Actions/DBSDataDiscovery.py prod/global /MuonEG/Run2016B-23Sep2016-v3/MINIAOD
    """
    dbsInstance = sys.argv[1]
    dbsDataset = sys.argv[2]
    dbsSecondaryDataset = sys.argv[3] if len(sys.argv) == 4 else None
    DBSUrl = 'https://cmsweb.cern.ch/dbs/%s/DBSReader/' % dbsInstance

    logging.basicConfig(level=logging.DEBUG)
    from WMCore.Configuration import ConfigurationEx
    from ServerUtilities import newX509env

    config = ConfigurationEx()
    config.section_("Services")
    config.section_("TaskWorker")
    # will use X509_USER_PROXY var for this test
    #config.TaskWorker.cmscert = os.environ["X509_USER_PROXY"]
    #config.TaskWorker.cmskey = os.environ["X509_USER_PROXY"]

    # will user service cert as defined for TW
    config.TaskWorker.cmscert = os.environ["X509_USER_CERT"]
    config.TaskWorker.cmskey = os.environ["X509_USER_KEY"]
    config.TaskWorker.envForCMSWEB = newX509env(X509_USER_CERT=config.TaskWorker.cmscert,
                                                X509_USER_KEY=config.TaskWorker.cmskey)

    config.TaskWorker.DDMServer = 'dynamo.mit.edu'
    config.TaskWorker.instance = 'prod'
    #config.TaskWorker.restHost = 'cmsweb.cern.ch'
    # The second word identifies the DB instance defined in CRABServerAuth.py on the REST
    #config.TaskWorker.restURInoAPI = '/crabserver/prod/'

    config.Services.Rucio_host = 'https://cms-rucio.cern.ch'
    config.Services.Rucio_account = 'crab_server'
    config.Services.Rucio_authUrl = 'https://cms-rucio-auth.cern.ch'
    config.Services.Rucio_caPath = '/etc/grid-security/certificates/'

    fileset = DBSDataDiscovery(config)
    fileset.execute(task={'tm_nonvalid_input_dataset': 'T', 'tm_use_parent': 0, 'user_proxy': 'None',
                          'tm_input_dataset': dbsDataset, 'tm_secondary_input_dataset': dbsSecondaryDataset, 'tm_taskname': 'pippo1',
                          'tm_split_algo' : 'automatic', 'tm_split_args' : {'runs':[], 'lumis':[]},
                          'tm_dbs_url': DBSUrl}, tempDir='')
    
#===============================================================================
#    Some interesting datasets for testing
#    dataset = '/DoubleMuon/Run2018B-PromptReco-v2/AOD'       # on tape
#    dataset = '/DoubleMuon/Run2018B-02Apr2020-v1/NANOAOD'    # isNano
#    dataset = '/DoubleMuon/Run2018B-17Sep2018-v1/MINIAOD'    # parent of above NANOAOD (for secondaryDataset lookup)
#    dataset = '/MuonEG/Run2016B-07Aug17_ver2-v1/AOD'         # no Nano on disk (at least atm)
#    dataset = '/MuonEG/Run2016B-v1/RAW'                      # on tape
#    dataset = '/MuonEG/Run2016B-23Sep2016-v3/MINIAOD'        # no NANO on disk (MINIAOD should always be on disk)
#===============================================================================
