import os
import pprint
import logging
from httplib import HTTPException

from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.WorkQueue.WorkQueueUtils import get_dbs
from WMCore.Services.DBS.DBSErrors import DBSReaderError
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
            raise TaskWorkerException("Cannot find dataset %s in DBS" % dataset)
        res = res[0]
        self.logger.info("Input dataset details: %s" % pprint.pformat(res))
        accessType = res['dataset_access_type']
        if accessType != 'VALID':
            msg = "The dataset you are analyzing is not 'VALID' but '%s'. CRAB3 will try to see if there is any valid file" % accessType
            if accessType == 'DEPRECATED': #as per Dima's suggestion https://github.com/dmwm/CRABServer/issues/4739
                msg += "Please, contact your physics group if you think the dataset should not be deprecated"
            self.uploadWarning(msg, kwargs['task']['user_proxy'], kwargs['task']['tm_taskname'])


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
        self.checkDatasetStatus(kwargs['task']['tm_input_dataset'], kwargs)
        try:
            # Get the list of blocks for the locations and then call dls.
            # The WMCore DBS3 implementation makes one call to dls for each block
            # with locations = True so we are using locations=False and looking up location later
            blocks = [ x['Name'] for x in self.dbs.getFileBlocksInfo(kwargs['task']['tm_input_dataset'], locations=False)]
        except DBSReaderError as dbsexc:
            #dataset not found in DBS is a known use case
            if str(dbsexc).find('No matching data'):
                raise TaskWorkerException("The CRAB3 server backend could not find dataset %s in this DBS instance: %s" % (kwargs['task']['tm_input_dataset'], dbsurl))
            raise
        #Create a map for block's locations: for each block get the list of locations
        try:
            locationsMap = self.dbs.listFileBlockLocation(list(blocks), phedexNodes=True)
        except Exception as ex: #TODO should we catch HttpException instead?
            self.logger.exception(ex)
            raise TaskWorkerException("The CRAB3 server backend could not get the location of the files from dbs or phedex.\n"+\
                                "This is could be a temporary phedex/dbs glitch, please try to submit a new task (resubmit will not work)"+\
                                " and contact the experts if the error persists.\nError reason: %s" % str(ex))
        self.keepOnlyDisks(locationsMap)
        if not locationsMap:
            msg = "Task could not be submitted because there is no DISK replica for dataset %s ." % (kwargs['task']['tm_input_dataset'])
            msg += " Please, check DAS, https://cmsweb.cern.ch/das, and make sure the dataset is accessible on DISK"
            msg += " You might want to contact your physics group if you need a disk replica."
            if self.otherLocations:
                msg += "\nN.B.: your dataset is stored at %s, but those are TAPE locations." % ','.join(sorted(self.otherLocations))
            raise TaskWorkerException(msg)
        if len(blocks) != len(locationsMap):
            self.logger.warning("The locations of some blocks have not been found: %s" % (set(blocks) - set(locationsMap)))
        try:
            filedetails = self.dbs.listDatasetFileDetails(kwargs['task']['tm_input_dataset'], True)
        except Exception as ex: #TODO should we catch HttpException instead?
            self.logger.exception(ex)
            raise TaskWorkerException("The CRAB3 server backend could not contact DBS to get the files deteails (Lumis, events, etc).\n"+\
                                "This is could be a temporary DBS glitch, please try to submit a new task (resubmit will not work)"+\
                                " and contact the experts if the error persists.\nError reason: %s" % str(ex)) #TODO addo the nodes phedex so the user can check themselves
        if not filedetails:
            raise TaskWorkerException("Cannot find any valid file inside the dataset. Please, check your dataset in DAS, https://cmsweb.cern.ch/das.\n"+\
                                      "Aborting submission. Resubmitting your task will not help.")
        result = self.formatOutput(task = kwargs['task'], requestname = kwargs['task']['tm_taskname'], datasetfiles = filedetails, locations = locationsMap)
        self.logger.debug("Got %s files" % len(result.result.getFiles()))
        return result

if __name__ == '__main__':
    logging.basicConfig(level = logging.DEBUG)
    datasets = ['/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO',
                '/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO',
                '/SingleMu/Run2012C-PromptReco-v2/AOD',
                '/SingleMu/Run2012D-PromptReco-v1/AOD',
                '/DYJetsToLL_M-50_TuneZ2Star_8TeV-madgraph-tarball/Summer12_DR53X-PU_S10_START53_V7A-v1/AODSIM',
                '/WJetsToLNu_TuneZ2Star_8TeV-madgraph-tarball/Summer12_DR53X-PU_S10_START53_V7A-v2/AODSIM',
                '/TauPlusX/Run2012D-PromptReco-v1/AOD']
    from WMCore.Configuration import Configuration
    config = Configuration()
    config.section_("Services")
    config.Services.DBSUrl = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'

#    config.Services.DBSUrl = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
#    config.Services.DBSUrl = 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSWriter/'
    config.section_("TaskWorker")
    #will use X509_USER_PROXY var for this test
    config.TaskWorker.cmscert = os.environ["X509_USER_PROXY"]
    config.TaskWorker.cmskey = os.environ["X509_USER_PROXY"]

    fileset = DBSDataDiscovery(config)
#    dataset = '/QCD_Pt-1800_Tune4C_13TeV_pythia8/Spring14dr-castor_PU_S14_POSTLS170_V6-v1/GEN-SIM-RECODEBUG'
    dataset = '/MB8TeVEtanoCasHFShoLib/Summer12-EflowHpu_NoPileUp_START53_V16-v1/RECODEBUG'
    fileset.execute(task={'tm_input_dataset':dataset, 'tm_taskname':'pippo1', 'tm_dbs_url': config.Services.DBSUrl})

#    dataset = '/DoubleElectron/Run2012C-22Jan2013-v1/RECO' #not in FNAL_DISK
#    fileset = DBSDataDiscovery(config)
#    dataset = '/SingleMu/Run2012D-22Jan2013-v1/AOD' #invalid file: /store/data/Run2012D/SingleMu/AOD/22Jan2013-v1/20001/7200FA02-CC85-E211-9966-001E4F3F165E.root
#    fileset.execute(task={'tm_input_dataset':dataset, 'tm_taskname':'pippo1', 'tm_dbs_url': config.Services.DBSUrl})

    for dataset in datasets:
        fileset = DBSDataDiscovery(config)
        print fileset.execute(task={'tm_input_dataset':dataset, 'tm_taskname':'pippo1', 'tm_dbs_url': config.Services.DBSUrl})

    #check local dbs use case
    config.Services.DBSUrl = 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSReader/'
    fileset = DBSDataDiscovery(config)
    dataset = '/GenericTTbar/hernan-140317_231446_crab_JH_ASO_test_T2_ES_CIEMAT_5000_100_140318_0014-ea0972193530f531086947d06eb0f121/USER'
    fileset.execute(task={'tm_input_dataset':dataset, 'tm_taskname':'pippo1', 'tm_dbs_url': config.Services.DBSUrl})

