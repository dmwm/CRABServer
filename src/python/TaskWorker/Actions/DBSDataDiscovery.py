
import os
import urllib
import logging
from httplib import HTTPException
from base64 import b64encode

from WMCore.WorkQueue.WorkQueueUtils import get_dbs

from TaskWorker.Actions.DataDiscovery import DataDiscovery
from TaskWorker.WorkerExceptions import StopHandler


class DBSDataDiscovery(DataDiscovery):
    """Performing the data discovery through CMS DBS service."""

    def execute(self, *args, **kwargs):
        self.logger.info("Data discovery with DBS") ## to be changed into debug
        old_cert_val = os.getenv("X509_USER_CERT")
        old_key_val = os.getenv("X509_USER_KEY")
        os.environ['X509_USER_CERT'] = self.config.TaskWorker.cmscert
        os.environ['X509_USER_KEY'] = self.config.TaskWorker.cmskey
        # DBS3 requires X509_USER_CERT to be set - but we don't want to leak that to other modules
        dbs = get_dbs(self.config.Services.DBSUrl)
        if kwargs['task']['tm_dbs_url']:
            dbs = get_dbs(kwargs['task']['tm_dbs_url'])
        #
        if old_cert_val != None:
            os.environ['X509_USER_CERT'] = old_cert_val
        if old_key_val != None:
            os.environ['X509_USER_KEY'] = old_key_val
        self.logger.debug("Data discovery through %s for %s" %(dbs, kwargs['task']['tm_taskname']))
        # Get the list of blocks for the locations and then call dls.
        # The WMCore DBS3 implementation makes one call to dls for each block
        # with locations = True
        blocks = [ x['Name'] for x in dbs.getFileBlocksInfo(kwargs['task']['tm_input_dataset'], locations=False)]
        #Create a map for block's locations: for each block get the list of locations
        ll = dbs.dls.getLocations(list(blocks),  showProd = True)
        if len(ll) == 0:
            msg = "No location was found for %s in %s." %(kwargs['task']['tm_input_dataset'],kwargs['task']['tm_dbs_url'])
            self.logger.error("Setting %s as failed" % str(kwargs['task']['tm_taskname']))
            configreq = {'workflow': kwargs['task']['tm_taskname'],
                         'status': "FAILED",
                         'subresource': 'failure',
                         'failure': b64encode(msg)}
            self.server.post(self.resturl, data = urllib.urlencode(configreq))
            raise StopHandler(msg)
        locations = map(lambda x: map(lambda y: y.host, x.locations), ll)
        locationsmap = dict(zip(blocks, locations))
        filedetails = dbs.listDatasetFileDetails(kwargs['task']['tm_input_dataset'], True)
        self.logger.debug("Got blocks: %s" % blocks[1:10])
        self.logger.debug("Got Locations: %s" % ll)
        self.logger.debug("Got Locations2: %s" % locations)
        self.logger.debug("Got locationsmap: %s" % locationsmap)
        result = self.formatOutput(task=kwargs['task'], requestname=kwargs['task']['tm_taskname'], datasetfiles=filedetails, locations=locationsmap)
        self.logger.debug("Got result: %s" % result.result)
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
    #config.Services.DBSUrl = 'https://cmsweb.cern.ch/dbs/dev/global/DBSReader'
    config.Services.DBSUrl = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
    for dataset in datasets:
        fileset = DBSDataDiscovery(config)
        print fileset.execute(task={'tm_input_dataset':dataset, 'tm_taskname':'pippo1', 'tm_dbs_url': config.Services.DBSUrl})

