import os
import sys
import pprint
import logging
from httplib import HTTPException

from WMCore.DataStructs.LumiList import LumiList
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.WorkQueue.WorkQueueUtils import get_dbs
from WMCore.Services.DBS.DBSErrors import DBSReaderError

from ServerUtilities import FEEDBACKMAIL
from TaskWorker.WorkerExceptions import TaskWorkerException
from TaskWorker.Actions.DataDiscovery import DataDiscovery


class DBSDataDiscovery(DataDiscovery):
    """Performing the data discovery through CMS DBS service.
    """

    def checkDatasetStatus(self, dataset, kwargs):
        res = self.dbs.dbs.listDatasets(dataset=dataset, detail=1, dataset_access_type='*')
        if len(res) > 1:
            raise TaskWorkerException("Found more than one dataset while checking in DBS the status of %s" % dataset)
        if len(res) == 0:
            raise TaskWorkerException("Cannot find dataset %s in %s DBS instance" % (dataset, self.dbsInstance))
        res = res[0]
        self.logger.info("Input dataset details: %s" % pprint.pformat(res))
        accessType = res['dataset_access_type']
        if accessType != 'VALID':
            #as per Dima's suggestion https://github.com/dmwm/CRABServer/issues/4739
            msgForDeprecDS = "Please contact your physics group if you think the dataset should not be deprecated."
            if kwargs['task']['tm_nonvalid_input_dataset'] != 'T':
                msg  = "CRAB refuses to proceed in getting the details of the dataset %s from DBS, because the dataset is not 'VALID' but '%s'." % (dataset, accessType)
                if accessType == 'DEPRECATED':
                    msg += " (%s)" % (msgForDeprecDS)
                msg += " To allow CRAB to consider a dataset that is not 'VALID', set Data.allowNonValidInputDataset = True in the CRAB configuration."
                msg += " Notice that this will not force CRAB to run over all files in the dataset;"
                msg += " CRAB will still check if there are any valid files in the dataset and run only over those files."
                raise TaskWorkerException(msg)
            msg  = "The input dataset %s is not 'VALID' but '%s'." % (dataset, accessType)
            msg += " CRAB will check if there are any valid files in the dataset and run only over those files."
            if accessType == 'DEPRECATED':
                msg += " %s" % (msgForDeprecDS)
            self.uploadWarning(msg, kwargs['task']['user_proxy'], kwargs['task']['tm_taskname'])
        return


    def keepOnlyDisks(self, locationsMap):
        self.otherLocations = set()
        phedex = PhEDEx() #TODO use certs from the config!
        #get all the PNN that are of kind disk
        try:
            diskLocations = set([pnn['name'] for pnn in phedex.getNodeMap()['phedex']['node'] if pnn['kind']=='Disk'])
        except HTTPException as ex:
            self.logger.error(ex.headers)
            raise TaskWorkerException("The CRAB3 server backend could not contact phedex to get the list of site storages.\n"+\
                                "This is could be a temporary phedex glitch, please try to submit a new task (resubmit will not work)"+\
                                " and contact the experts if the error persists.\nError reason: %s" % str(ex)) #TODO addo the nodes phedex so the user can check themselves
        for block, locations in locationsMap.iteritems():
            locationsMap[block] = set(locations) & diskLocations
            self.otherLocations = self.otherLocations.union(set(locations) - diskLocations)
        #remove any key with value that has set([])
        for key, value in locationsMap.items(): #wont work in python3!
            if value == set([]):
                locationsMap.pop(key)

    def execute(self, *args, **kwargs):
        self.logger.info("Data discovery with DBS") ## to be changed into debug
        old_cert_val = os.getenv("X509_USER_CERT")
        old_key_val = os.getenv("X509_USER_KEY")
        os.environ['X509_USER_CERT'] = self.config.TaskWorker.cmscert
        os.environ['X509_USER_KEY'] = self.config.TaskWorker.cmskey
        # DBS3 requires X509_USER_CERT to be set - but we don't want to leak that to other modules
        dbsurl = self.config.Services.DBSUrl
        if kwargs['task']['tm_dbs_url']:
            dbsurl = kwargs['task']['tm_dbs_url']
        self.dbs = get_dbs(dbsurl)
        self.dbsInstance = self.dbs.dbs.serverinfo()["dbs_instance"]
        #
        if old_cert_val != None:
            os.environ['X509_USER_CERT'] = old_cert_val
        else:
            del os.environ['X509_USER_CERT']
        if old_key_val != None:
            os.environ['X509_USER_KEY'] = old_key_val
        else:
            del os.environ['X509_USER_KEY']

        self.logger.debug("Data discovery through %s for %s" %(self.dbs, kwargs['task']['tm_taskname']))

        inputDataset = kwargs['task']['tm_input_dataset']

        self.checkDatasetStatus(inputDataset, kwargs)

        try:
            # Get the list of blocks for the locations and then call dls.
            # The WMCore DBS3 implementation makes one call to dls for each block
            # with locations = True so we are using locations=False and looking up location later
            blocks = [x['Name'] for x in self.dbs.getFileBlocksInfo(inputDataset, locations=False)]
        except DBSReaderError as dbsexc:
            #dataset not found in DBS is a known use case
            if str(dbsexc).find('No matching data'):
                raise TaskWorkerException("The CRAB3 server backend could not find dataset %s in this DBS instance: %s" % (inputDataset, dbsurl))
            raise

        ## Create a map for block's locations: for each block get the list of locations.
        ## Note: listFileBlockLocation() gets first the locations from PhEDEx, and if no
        ## locations are found it gets the original locations from DBS. So it should
        ## never be the case at this point that some blocks have no locations.
        try:
            locationsMap = self.dbs.listFileBlockLocation(list(blocks), phedexNodes=True)
        except Exception as ex: #TODO should we catch HttpException instead?
            self.logger.exception(ex)
            msg  = "The CRAB3 server backend could not contact DBS/PhEDEx to get the locations of the files in the input dataset."
            msg += " This could be a temporary DBS/PhEDEx glitch."
            msg += " Please wait a minute and try to resubmit the task."
            msg += " If the error persists, report the issue at %s." % (FEEDBACKMAIL)
            msg += "\nError reason: %s" % (str(ex))
            raise TaskWorkerException(msg)
        self.keepOnlyDisks(locationsMap)
        if not locationsMap:
            msg = "Task could not be submitted, because there is no DISK replica for dataset %s ." % (inputDataset)
            msg += " Please, check DAS, https://cmsweb.cern.ch/das, and make sure the dataset is accessible on DISK."
            msg += " You might want to contact your physics group if you need a disk replica."
            if self.otherLocations:
                msg += "\nN.B.: your dataset is stored at %s, but those are TAPE locations." % ','.join(sorted(self.otherLocations))
            raise TaskWorkerException(msg)
        if len(blocks) != len(locationsMap):
            self.logger.warning("The locations of some blocks have not been found: %s" % (set(blocks) - set(locationsMap)))

        ## Get detailed information about the files in the input dataset.
        try:
            inputFilesDetails = self.dbs.listDatasetFileDetails(inputDataset, getParents=True, validFileOnly=0)
        except Exception as ex: #TODO should we catch HttpException instead?
            self.logger.exception(ex)
            msg  = "The CRAB3 server backend could not contact DBS to get detailed information of files in the input dataset."
            msg += " This could be a temporary DBS glitch."
            msg += " Please wait a minute and try to resubmit the task."
            msg += " If the error persists, report the issue at %s." % (FEEDBACKMAIL)
            msg += "\nError reason: %s" % (str(ex))
            raise TaskWorkerException(msg) #TODO addo the nodes phedex so the user can check themselves
        if not inputFilesDetails:
            msg  = "Cannot find any file inside the input dataset."
            msg += " Please check the dataset in DAS: %s." \
                 % (("https://cmsweb.cern.ch/das/request?instance=%s&input=dataset=%s") % (self.dbsInstance, inputDataset))
            msg += "\nAborting submission. Resubmission will not help."
            raise TaskWorkerException(msg)

        ## If the task needs the parent files of the input files, get here the lumis of
        ## the parent files so that after job splitting we can filter out parent files
        ## that have no lumis in common with the input files.
        if kwargs['task']['tm_use_parent'] == 1:
            validInputFilesLFNs = [lfn for lfn in inputFilesDetails if inputFilesDetails[lfn].get('ValidFile', True)]
            ## Get the details of the parent files of valid input files.
            parentFilesDetails = {}
            for blockName in blocks:
                try:
                    inputFilesInBlockInfo = self.dbs.listFilesInBlockWithParents(blockName, lumis=True, validFileOnly=0)
                except Exception as ex: #TODO should we catch HttpException instead?
                    self.logger.exception(ex)
                    msg  = "The CRAB3 server backend could not contact DBS to get detailed information about the parents of files in block %s of the input dataset." % (blockName)
                    msg += " This could be a temporary DBS glitch."
                    msg += " Please wait a minute and try to resubmit the task."
                    msg += " If the error persists, report the issue at %s." % (FEEDBACKMAIL)
                    msg += "\nError reason: %s" % (str(ex))
                    raise TaskWorkerException(msg)
                for inputFileInfo in inputFilesInBlockInfo:
                    if inputFileInfo['logical_file_name'] not in validInputFilesLFNs:
                        continue
                    parentFilesDetails[inputFileInfo['logical_file_name']] = inputFileInfo['ParentList']
            ## Check that we got parent files details for all the valid input files in the
            ## the reference list 'validInputFilesLFNs' (derived from 'inputFilesDetails').
            missing = list(set(validInputFilesLFNs) - set(parentFilesDetails))
            if missing:
                msg = "Could not get detailed information for the parents of the following input files: %s" % (missing)
                raise TaskWorkerException(msg)
            ## For each (valid) input file:
            for inputFileLFN, parentFilesInfo in parentFilesDetails.iteritems():
                ## Check that we got the details of all its parent files.
                missing = list(set(inputFilesDetails[inputFileLFN]['Parents']) - set(map(lambda x: x['logical_file_name'], parentFilesInfo)))
                if missing:
                    msg = "Could not get detailed information for the following parents files of input file %s: %s" % (inputFileLFN, missing)
                    raise TaskWorkerException(msg)
                ## Get and save the lumis of each parent file.
                inputFilesDetails[inputFileLFN]['ParentsLumis'] = {}
                for parentInfo in parentFilesInfo:
                    if parentInfo['logical_file_name'] not in inputFilesDetails[inputFileLFN]['Parents']:
                        continue
                    parentLumis = {}
                    for run, lumis in map(lambda x: (x['RunNumber'], x['LumiSectionNumber']), parentInfo['LumiList']):
                        parentLumis.setdefault(run, []).extend(lumis)
                    parentLumis = LumiList(runsAndLumis=parentLumis)
                    inputFilesDetails[inputFileLFN]['ParentsLumis'][parentInfo['logical_file_name']] = parentLumis.getCompactList()

        secondaryInputDataset = kwargs['task']['tm_secondary_input_dataset']
        if secondaryInputDataset:
            ## Get detailed information about the files in the secondary input dataset.
            try:
                secondaryInputFilesDetails = self.dbs.listDatasetFileDetails(secondaryInputDataset, getParents=False, validFileOnly=0)
            except Exception as ex: #TODO should we catch HttpException instead?
                self.logger.exception(ex)
                msg  = "The CRAB3 server backend could not contact DBS to get detailed information of files in the secondary input dataset."
                msg += " This could be a temporary DBS glitch."
                msg += " Please wait a minute and try to resubmit the task."
                msg += " If the error persists, report the issue at %s." % (FEEDBACKMAIL)
                msg += "\nError reason: %s" % (str(ex))
                raise TaskWorkerException(msg) #TODO addo the nodes phedex so the user can check themselves
            if not secondaryInputFilesDetails:
                msg  = "Cannot find any file inside the secondary input dataset."
                msg += " Please check the dataset in DAS: %s." \
                     % (("https://cmsweb.cern.ch/das/request?instance=%s&input=dataset=%s") % (self.dbsInstance, secondaryInputDataset))
                msg += "\nAborting submission. Resubmission will not help."
                raise TaskWorkerException(msg)

            for secfilename, secinfos in secondaryInputFilesDetails.items():
                secinfos['lumiobj'] = LumiList(runsAndLumis=secinfos['Lumis'])

            self.logger.info("Beginning to match files from secondary dataset")
            for filename, infos in inputFilesDetails.items():
                infos['Parents'] = []
                infos['ParentsLumis'] = {}
                lumis = LumiList(runsAndLumis=infos['Lumis'])
                for secfilename, secinfos in secondaryInputFilesDetails.items():
                    if (lumis & secinfos['lumiobj']):
                        infos['Parents'].append(secfilename)
                        infos['ParentsLumis'][secfilename] = secinfos['lumiobj'].getCompactList()
            self.logger.info("Done matching files from secondary dataset")
            kwargs['task']['tm_use_parent'] = 1

        ## Format the output creating the data structures required by wmcore. Filters out invalid files,
        ## files whose block has no location, and figures out the PSN
        result = self.formatOutput(task = kwargs['task'], requestname = kwargs['task']['tm_taskname'], datasetfiles = inputFilesDetails, locations = locationsMap)

        if not result.result:
            msg  = "Cannot find any valid file inside the input dataset."
            msg += " Please check the dataset in DAS: %s." \
                 % (("https://cmsweb.cern.ch/das/request?instance=%s&input=dataset=%s") % (self.dbsInstance, inputDataset))
            msg += "\nAborting submission. Resubmission will not help."
            raise TaskWorkerException(msg)

        self.logger.debug("Got %s files" % len(result.result.getFiles()))
        return result

if __name__ == '__main__':
    """ Usage: python DBSDataDiscovery.py dbs_instance dataset
        where dbs_instance should be either prod or phys03

        Example: python ~/repos/CRABServer/src/python/TaskWorker/Actions/DBSDataDiscovery.py phys03 /MinBias/jmsilva-crab_scale_70633-3d12352c28d6995a3700097dc8082c04/USER

        Note: self.uploadWarning is failing, I usually comment it when I run this script standalone
    """
    dbsInstance = sys.argv[1]
    dataset = sys.argv[2]

    logging.basicConfig(level = logging.DEBUG)
    from WMCore.Configuration import Configuration
    config = Configuration()
    config.section_("Services")
    config.Services.DBSUrl = 'https://cmsweb.cern.ch/dbs/%s/DBSReader/' % dbsInstance
    config.section_("TaskWorker")
    # will use X509_USER_PROXY var for this test
    config.TaskWorker.cmscert = os.environ["X509_USER_PROXY"]
    config.TaskWorker.cmskey = os.environ["X509_USER_PROXY"]

    fileset = DBSDataDiscovery(config)
    fileset.execute(task={'tm_nonvalid_input_dataset': 'T', 'tm_use_parent': 0, 'user_proxy': os.environ["X509_USER_PROXY"],
                          'tm_input_dataset': dataset, 'tm_taskname': 'pippo1', 'tm_dbs_url': config.Services.DBSUrl})

#===============================================================================
#    Some interesting datasets that were hardcoded here (MM I am not using them actually, maybe we could delete them?)
#    dataset = '/QCD_Pt-1800_Tune4C_13TeV_pythia8/Spring14dr-castor_PU_S14_POSTLS170_V6-v1/GEN-SIM-RECODEBUG'
#    dataset = '/SingleMu/Run2012D-22Jan2013-v1/AOD' #invalid file: /store/data/Run2012D/SingleMu/AOD/22Jan2013-v1/20001/7200FA02-CC85-E211-9966-001E4F3F165E.root
#    dataset = '/MB8TeVEtanoCasHFShoLib/Summer12-EflowHpu_NoPileUp_START53_V16-v1/RECODEBUG'
#    dataset = '/DoubleElectron/Run2012C-22Jan2013-v1/RECO' #not in FNAL_DISK
#    dataset = '/GenericTTbar/atanasi-140429_131619_crab_AprilTest_3000jbsOf3hrs-95e5dc29a1ac0766eb8514eb5d4ff77a/USER' # This dataset is INVALID, but has 1 valid file: /store/user/atanasi/GenericTTbar/140429_131619_crab_AprilTest_3000jbsOf3hrs/140429_131619/0000/MyTTBarTauolaTest_1.root, which belongs to block /GenericTTbar/atanasi-140429_131619_crab_AprilTest_3000jbsOf3hrs-95e5dc29a1ac0766eb8514eb5d4ff77a/USER#c4dea6b6-3e26-4d3a-a6c4-ab1a5b0a5f4c, which has 65 files (64 are invalid).
#    dataset = '/MinBias/jmsilva-crab_scale_70633-3d12352c28d6995a3700097dc8082c04/USER'
#    dataset = '/SingleMu/atanasi-CRAB3-tutorial_Data-analysis_LumiList-f765a0f4fbf582146a505cfe3fd08f3e/USER'
#    datasets = ['/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO',
#                '/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO',
#                '/SingleMu/Run2012C-PromptReco-v2/AOD',
#                '/SingleMu/Run2012D-PromptReco-v1/AOD',
#                '/DYJetsToLL_M-50_TuneZ2Star_8TeV-madgraph-tarball/Summer12_DR53X-PU_S10_START53_V7A-v1/AODSIM',
#                '/WJetsToLNu_TuneZ2Star_8TeV-madgraph-tarball/Summer12_DR53X-PU_S10_START53_V7A-v2/AODSIM',
#                '/TauPlusX/Run2012D-PromptReco-v1/AOD']
#===============================================================================
