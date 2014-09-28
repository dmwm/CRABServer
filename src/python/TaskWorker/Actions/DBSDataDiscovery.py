
import os
import urllib
import logging
from httplib import HTTPException
from base64 import b64encode

from WMCore.WorkQueue.WorkQueueUtils import get_dbs
from WMCore.Services.DBS.DBSErrors import DBSReaderError
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from TaskWorker.WorkerExceptions import TaskWorkerException

from TaskWorker.Actions.DataDiscovery import DataDiscovery
from TaskWorker.WorkerExceptions import StopHandler

class DBSDataDiscovery(DataDiscovery):
    """Performing the data discovery through CMS DBS service."""

    def keepOnlyDisks(self, locationsMap):
        phedex = PhEDEx()
        #get all the PNN that are of kind disk
        diskLocations = set([pnn['name'] for pnn in phedex.getNodeMap()['phedex']['node'] if pnn['kind']=='Disk'])
        for block, locations in locationsMap.iteritems():
            locationsMap[block] = set(locations) & diskLocations

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
        dbs = get_dbs(dbsurl)
        #
        if old_cert_val != None:
            os.environ['X509_USER_CERT'] = old_cert_val
        else:
            del os.environ['X509_USER_CERT']
        if old_key_val != None:
            os.environ['X509_USER_KEY'] = old_key_val
        else:
            del os.environ['X509_USER_KEY']
        self.logger.debug("Data discovery through %s for %s" %(dbs, kwargs['task']['tm_taskname']))
        try:
            # Get the list of blocks for the locations and then call dls.
            # The WMCore DBS3 implementation makes one call to dls for each block
            # with locations = True so we are using locations=False and looking up location later
            blocks = [ x['Name'] for x in dbs.getFileBlocksInfo(kwargs['task']['tm_input_dataset'], locations=False)]
        except DBSReaderError, dbsexc:
            #dataset not found in DBS is a known use case
            if str(dbsexc).find('No matching data'):
                raise TaskWorkerException("Cannot find dataset %s in this DBS instance: %s" % (kwargs['task']['tm_input_dataset'], dbsurl))
            raise
        #Create a map for block's locations: for each block get the list of locations
        locationsMap = dbs.listFileBlockLocation(list(blocks), phedexNodes=True)
        self.keepOnlyDisks(locationsMap)
        if not locationsMap:
            msg = "No location was found for %s in %s." %(kwargs['task']['tm_input_dataset'], dbsurl)
#           You should not need the following if you raise TaskWorkerException
#            self.logger.error("Setting %s as failed" % str(kwargs['task']['tm_taskname']))
#            configreq = {'workflow': kwargs['task']['tm_taskname'],
#                         'status': "FAILED",
#                         'subresource': 'failure',
#                         'failure': b64encode(msg)}
#            self.server.post(self.resturl, data = urllib.urlencode(configreq))
            raise TaskWorkerException(msg)
        if len(blocks) != len(locationsMap):
            self.logger.warning("The locations of some blocks have not been found: %s" % (set(blocks) - set(locationsMap)))
        filedetails = dbs.listDatasetFileDetails(kwargs['task']['tm_input_dataset'], True)
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
    datasets = [ '/MuOnia/Run2010A-v1/RAW' ]
    from WMCore.Configuration import Configuration
    config = Configuration()
    config.section_("Services")
    config.Services.DBSUrl = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'

    #config.Services.DBSUrl = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
    #config.Services.DBSUrl = 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSWriter/'
    config.section_("TaskWorker")
    #will use X509_USER_PROXY var for this test
    config.TaskWorker.cmscert = os.environ["X509_USER_PROXY"]
    config.TaskWorker.cmskey = os.environ["X509_USER_PROXY"]

    fileset = DBSDataDiscovery(config)
    dataset = '/DoubleMuParked/Run2012B-22Jan2013-v1/AOD' #No locations for invalid block /DoubleMuParked/Run2012B-22Jan2013-v1/AOD#945791b2-da34-11e2-b22d-00221959e69e
    fileset.execute(task={'tm_input_dataset':dataset, 'tm_taskname':'pippo1', 'tm_dbs_url': config.Services.DBSUrl})

    #dataset = '/DoubleElectron/Run2012C-22Jan2013-v1/RECO' #not in FNAL_DISK
    fileset = DBSDataDiscovery(config)
    dataset = '/SingleMu/Run2012D-22Jan2013-v1/AOD' #invalid file: /store/data/Run2012D/SingleMu/AOD/22Jan2013-v1/20001/7200FA02-CC85-E211-9966-001E4F3F165E.root
    fileset.execute(task={'tm_input_dataset':dataset, 'tm_taskname':'pippo1', 'tm_dbs_url': config.Services.DBSUrl})

    for dataset in datasets:
        fileset = DBSDataDiscovery(config)
        print fileset.execute(task={'tm_input_dataset':dataset, 'tm_taskname':'pippo1', 'tm_dbs_url': config.Services.DBSUrl})

    #check local dbs use case
    config.Services.DBSUrl = 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSReader/'
    fileset = DBSDataDiscovery(config)
    dataset = '/GenericTTbar/hernan-140317_231446_crab_JH_ASO_test_T2_ES_CIEMAT_5000_100_140318_0014-ea0972193530f531086947d06eb0f121/USER'
    fileset.execute(task={'tm_input_dataset':dataset, 'tm_taskname':'pippo1', 'tm_dbs_url': config.Services.DBSUrl})

