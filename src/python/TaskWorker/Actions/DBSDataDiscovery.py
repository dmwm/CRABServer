from __future__ import print_function
import os
import sys
import pprint
import logging
import json
from httplib import HTTPException
import urllib

from WMCore.DataStructs.LumiList import LumiList
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.Services.DBS.DBSErrors import DBSReaderError
from TaskWorker.WorkerExceptions import TaskWorkerException, TapeDatasetException

from TaskWorker.Actions.DataDiscovery import DataDiscovery

from RESTInteractions import HTTPRequests

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
        self.logger.info("Input dataset details: %s", pprint.pformat(res))
        accessType = res['dataset_access_type']
        if accessType != 'VALID':
            # as per Dima's suggestion https://github.com/dmwm/CRABServer/issues/4739
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


    def checkBlocksSize(self, blocks):
        """ Make sure no single blocks has more than 100k lumis. See
            https://hypernews.cern.ch/HyperNews/CMS/get/dmDevelopment/2022/1/1/1/1/1/1/2.html
        """
        MAX_LUMIS = 100000
        for block in blocks:
            blockInfo = self.dbs.getDBSSummaryInfo(block=block)
            if blockInfo['NumberOfLumis'] > MAX_LUMIS:
                msg = "Block %s contains more than %s lumis.\nThis blows up CRAB server memory" % (block, MAX_LUMIS)
                msg += "\nCRAB can only split this by ignoring lumi information. You can do this"
                msg += "\nusing FileBased split algorithm and avoiding any additional request"
                msg += "\nwich may cause lumi information to be looked up. See CRAB FAQ for more info:"
                msg += "\nhttps://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ"
                raise TaskWorkerException(msg)


    def execute(self, *args, **kwargs):
        self.logger.info("Data discovery with DBS") ## to be changed into debug
        old_cert_val = os.getenv("X509_USER_CERT")
        old_key_val = os.getenv("X509_USER_KEY")
        try:
            os.environ['X509_USER_CERT'] = self.config.TaskWorker.cmscert
            os.environ['X509_USER_KEY'] = self.config.TaskWorker.cmskey
            # DBS3 requires X509_USER_CERT to be set - but we don't want to leak that to other modules
            dbsurl = self.config.Services.DBSUrl
            if kwargs['task']['tm_dbs_url']:
                dbsurl = kwargs['task']['tm_dbs_url']
            self.dbs = DBSReader(dbsurl)
            self.dbsInstance = self.dbs.dbs.serverinfo()["dbs_instance"]
        finally:
            if old_cert_val != None:
                os.environ['X509_USER_CERT'] = old_cert_val
            else:
                del os.environ['X509_USER_CERT']
            if old_key_val != None:
                os.environ['X509_USER_KEY'] = old_key_val
            else:
                del os.environ['X509_USER_KEY']

        taskName = kwargs['task']['tm_taskname']
        self.logger.debug("Data discovery through %s for %s", self.dbs, taskName)

        inputDataset = kwargs['task']['tm_input_dataset']
        secondaryDataset = kwargs['task'].get('tm_secondary_input_dataset', None)
        
        self.checkDatasetStatus(inputDataset, kwargs)
        if secondaryDataset:
            self.checkDatasetStatus(secondaryDataset, kwargs)

        try:
            # Get the list of blocks for the locations and then call dls.
            # The WMCore DBS3 implementation makes one call to dls for each block
            # with locations = True so we are using locations=False and looking up location later
            blocks = [ x['Name'] for x in self.dbs.getFileBlocksInfo(inputDataset, locations=False)]
            if secondaryDataset:
                secondaryBlocks = [ x['Name'] for x in self.dbs.getFileBlocksInfo(secondaryDataset, locations=False)]
        except DBSReaderError as dbsexc:
            #dataset not found in DBS is a known use case
            if str(dbsexc).find('No matching data'):
                raise TaskWorkerException("CRAB could not find dataset %s in this DBS instance: %s" % inputDataset, dbsurl)
            raise
        ## Create a map for block's locations: for each block get the list of locations.
        ## Note: listFileBlockLocation() gets first the locations from PhEDEx, and if no
        ## locations are found it gets the original locations from DBS. So it should
        ## never be the case at this point that some blocks have no locations.
        ## locationsMap is a dictionary, key=blockName, value=list of PhedexNodes, example:
        ## {'/JetHT/Run2016B-PromptReco-v2/AOD#b10179dc-3723-11e6-9aa5-001e67abf228': [u'T1_IT_CNAF_Buffer', u'T2_US_Wisconsin', u'T1_IT_CNAF_MSS', u'T2_BE_UCL'],
        ## '/JetHT/Run2016B-PromptReco-v2/AOD#89b03ca6-1dc9-11e6-b567-001e67ac06a0': [u'T1_IT_CNAF_Buffer', u'T2_US_Wisconsin', u'T1_IT_CNAF_MSS', u'T2_BE_UCL'}
        try:
            dbsOnly = self.dbsInstance.split('/')[1] != 'global'
            locationsMap = self.dbs.listFileBlockLocation(list(blocks), dbsOnly=dbsOnly)
        except Exception as ex: #TODO should we catch HttpException instead?
            self.logger.exception(ex)
            raise TaskWorkerException("The CRAB3 server backend could not get the location of the files from dbs or phedex.\n"+\
                                "This is could be a temporary phedex/dbs glitch, please try to submit a new task (resubmit will not work)"+\
                                " and contact the experts if the error persists.\nError reason: %s" % str(ex))
        self.keepOnlyDisks(locationsMap)
        if not locationsMap:
            msg = "Task could not be submitted because there is no DISK replica for dataset %s" % inputDataset
            if self.otherLocations:
                msg += "\nN.B.: the input dataset is stored at %s, but those are TAPE locations." % ', '.join(sorted(self.otherLocations))
                # submit request to DDM
                site = "T2*" # will let Dynamo choose which T2 to stage the blocks to, TODO: allow the user to specify it
                DDMJson = json.dumps({"item": blocks, "site": site})
                commonURL =  'registry/request/'
                userServer = HTTPRequests(url=self.config.TaskWorker.DDMServer, localcert=self.config.TaskWorker.cmscert, localkey=self.config.TaskWorker.cmskey, verbose=False)
                DDMRequest = (userServer.post('/'+commonURL+'copy', data=DDMJson))[0]
                self.logger.info("Contacted %s using %s and %s, got:\n%s", self.config.TaskWorker.DDMServer, self.config.TaskWorker.cmscert, self.config.TaskWorker.cmskey, DDMRequest)
                # The query above returns a JSON with a format {"result": "OK", "message": "Copy requested", "data": [{"request_id": 18, "site": <site>, "item": [<list of blocks>], "group": "AnalysisOps", "n": 1, "status": "new", "first_request": "2018-02-26 23:57:37", "last_request": "2018-02-26 23:57:37", "request_count": 1}]}
                if DDMRequest["result"] == "OK":
                    msg += "\nA disk replica has been requested on %s" % DDMRequest["data"][0]["first_request"]
                    # set status to TAPERECALL
                    tapeRecallStatus = 'TAPERECALL'
                    server = HTTPRequests(url=self.config.TaskWorker.resturl, localcert=self.config.TaskWorker.cmscert, localkey=self.config.TaskWorker.cmskey, verbose=False)
                    configreq = {'workflow': taskName,
                                 'status': tapeRecallStatus,
                                 'subresource': 'state'
                    }
                    tapeRecallStatusSet = server.post(self.config.TaskWorker.resturi, data = urllib.urlencode(configreq))
                    if tapeRecallStatusSet[2] == "OK":
                        self.logger.info("Status for task %s set to '%s'", taskName, tapeRecallStatus)
                        msg += " and the task will be submitted as soon as it is completed."
                        self.uploadWarning(msg, kwargs['task']['user_proxy'], taskName)
                        raise TapeDatasetException(msg)
                    else:
                        msg += ", please try again in two days."

            msg += "\nPlease, check DAS (https://cmsweb.cern.ch/das) and make sure the dataset is accessible on DISK."
            msg += " You might want to contact your physics group if you need a disk replica."
            raise TaskWorkerException(msg)
        if len(blocks) != len(locationsMap):
            self.logger.warning("The locations of some blocks have not been found: %s", set(blocks) - set(locationsMap))
        
        # will not need lumi info if user has asked for split by file with no run/lumi mask
        splitAlgo = kwargs['task']['tm_split_algo']
        lumiMask  = kwargs['task']['tm_split_args']['lumis']
        runRange  = kwargs['task']['tm_split_args']['runs']

        needLumiInfo = splitAlgo != 'FileBased' or lumiMask != [] or runRange != []
        # secondary dataset access relies on run/lumi info
        if secondaryDataset: needLumiInfo = True

        if needLumiInfo:
            self.checkBlocksSize(blocks)
            if secondaryDataset:
                self.checkBlocksSize(secondaryBlocks)
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
                        if (lumis & secinfos['lumiobj']):
                            infos['Parents'].append(secfilename)
                self.logger.info("Done matching files from secondary dataset")
                kwargs['task']['tm_use_parent'] = 1
        except Exception as ex: #TODO should we catch HttpException instead?
            self.logger.exception(ex)
            raise TaskWorkerException("The CRAB3 server backend could not contact DBS to get the files details (Lumis, events, etc).\n"+\
                                "This is could be a temporary DBS glitch. Please try to submit a new task (resubmit will not work)"+\
                                " and contact the experts if the error persists.\nError reason: %s" % str(ex)) #TODO addo the nodes phedex so the user can check themselves
        if not filedetails:
            raise TaskWorkerException(("Cannot find any file inside the dataset. Please, check your dataset in DAS, %s.\n"
                                      "Aborting submission. Resubmitting your task will not help.") %
                                      ("https://cmsweb.cern.ch/das/request?instance=%s&input=dataset=%s") %
                                      (self.dbsInstance, inputDataset))

        ## Format the output creating the data structures required by wmcore. Filters out invalid files,
        ## files whose block has no location, and figures out the PSN
        result = self.formatOutput(task = kwargs['task'], requestname = taskName,
                                   datasetfiles = filedetails, locations = locationsMap,
                                   tempDir = kwargs['tempDir'])

        if not result.result:
            raise TaskWorkerException(("Cannot find any valid file inside the dataset. Please, check your dataset in DAS, %s.\n"
                                      "Aborting submission. Resubmitting your task will not help.") %
                                      ("https://cmsweb.cern.ch/das/request?instance=%s&input=dataset=%s") %
                                      (self.dbsInstance, inputDataset))

        self.logger.debug("Got %s files", len(result.result.getFiles()))
        return result

if __name__ == '__main__':
    """ Usage: python DBSDataDiscovery.py dbs_instance dbsDataset
        where dbs_instance should be either prod or phys03

        Example: python ~/repos/CRABServer/src/python/TaskWorker/Actions/DBSDataDiscovery.py prod/phys03 /MinBias/jmsilva-crab_scale_70633-3d12352c28d6995a3700097dc8082c04/USER

        Note: self.uploadWarning is failing, I usually comment it when I run this script standalone
    """
    dbsInstance = sys.argv[1]
    dbsDataset = sys.argv[2]

    logging.basicConfig(level = logging.DEBUG)
    from WMCore.Configuration import Configuration
    config = Configuration()
    config.section_("Services")
    config.Services.DBSUrl = 'https://cmsweb.cern.ch/dbs/%s/DBSReader/' % dbsInstance
    config.section_("TaskWorker")
    # will use X509_USER_PROXY var for this test
    #config.TaskWorker.cmscert = os.environ["X509_USER_PROXY"]
    #config.TaskWorker.cmskey = os.environ["X509_USER_PROXY"]

    # will user service cert as defined for TW
    config.TaskWorker.cmscert = os.environ["X509_USER_CERT"]
    config.TaskWorker.cmskey = os.environ["X509_USER_KEY"]

    config.TaskWorker.DDMServer = 'dynamo.mit.edu'
    config.TaskWorker.resturl = 'cmsweb.cern.ch'
    config.TaskWorker.resturi = '/crabserver/prod/workflowdb'

    fileset = DBSDataDiscovery(config)
    fileset.execute(task={'tm_nonvalid_input_dataset': 'T', 'tm_use_parent': 0, #'user_proxy': os.environ["X509_USER_PROXY"],
                          'tm_input_dataset': dbsDataset, 'tm_taskname': 'pippo1',
                          'tm_split_algo' : 'automatic', 'tm_split_args' : {'runs':[], 'lumis':[]},
                          'tm_dbs_url': config.Services.DBSUrl}, tempDir='')
    
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
