from __future__ import print_function
import os
import sys
import logging
from httplib import HTTPException
import urllib

from WMCore.DataStructs.LumiList import LumiList
from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.Services.DBS.DBSErrors import DBSReaderError

from TaskWorker.WorkerExceptions import TaskWorkerException, TapeDatasetException
from TaskWorker.Actions.DataDiscovery import DataDiscovery
from TaskWorker.Actions.DDMRequests import blocksRequest
from ServerUtilities import tempSetLogLevel

from rucio.common.exception import (DuplicateRule, DataIdentifierAlreadyExists, DuplicateContent, RuleNotFound)

class DBSDataDiscovery(DataDiscovery):
    """Performing the data discovery through CMS DBS service.
    """

    def __init__(self, config, server='', resturi='', procnum=-1, rucioClient=None):
        DataDiscovery.__init__(self, config, server, resturi, procnum)
        self.rucioClient = rucioClient

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
            # as of Sept 2020, tape RSEs ends with _Tape, go for the quick hack
            diskRSEs = [rse for rse in locations if not 'Tape' in rse]
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

    def requestTapeRecall(self, blockList=[], system='Dynamo', msgHead=''):   # pylint: disable=W0102
        """
        :param blockList: a list of blocks to recall from Tape to Disk
        :param system: a string identifying the DDM system to use 'Dynamo' or 'Rucio' or 'None'
        :param msgHead: a string with the initial part of a message to be used for exceptions
        Since data on tape means no submission possible, this function will always raise
        a TaskWorkerException to stop the action flow. The exception message contains details
        and an attempt is done to upload it to TaskDB so that crab status can report it
        """
        # start with old Dyanmo implementation. A new implementation with Rucio will come

        msg = msgHead
        if system == 'Rucio':
            # turn input CMS blocks into Rucio dids in cms scope
            dids = [{'scope': 'cms', 'name': block} for block in blockList]
            # prepare container /TapeRecall/taskname/USER in the service scope
            myScope = 'user.%s' % self.config.Services.Rucio_account
            containerName = '/TapeRecall/%s/USER' % self.taskName.replace(':', '.')
            containerDid = {'scope':myScope, 'name':containerName}
            self.logger.info("Create RUcio container %s", containerName)
            try:
                response = self.rucioClient.add_container(myScope, containerName)
            except DataIdentifierAlreadyExists:
                self.logger.debug("Container name already exists in Rucio. Keep going")
                pass
            except Exception as ex:
                msg += "Rucio exception creating container: %s" %  (str(ex))
                raise TaskWorkerException(msg)
            try:
                response = self.rucioClient.attach_dids(myScope, containerName, dids)
            except DuplicateContent:
                self.logger.debug("Some dids are already in this container. Keep going")
                pass
            except Exception as ex:
                msg += "Rucio exception adding blocks to container: %s" %  (str(ex))
                raise TaskWorkerException(msg)
            self.logger.info("Rucio container %s:%s created with %d blocks", myScope, containerName, len(blockList))

            # Compute size of recall request
            sizeToRecall = 0
            for block in blockList:
                replicas = self.rucioClient.list_dataset_replicas('cms', block)
                bytes = replicas.next()['bytes']  # pick first replica for each block, they better all have same size
                sizeToRecall += bytes
            TBtoRecall = sizeToRecall // 1e12
            if TBtoRecall > 0:
                self.logger.info("Total size of data to recall : %d TBytes", TBtoRecall)
            else:
                self.logger.info("Total size of data to recall : %d GBytes", sizeToRecall/1e9)

            if TBtoRecall > 30.:
                grouping = 'DATASET'  # Rucio DATASET i.e. CMS block !
                self.logger.info("Will scatter blocks on multiple sites")
            else:
                grouping = 'ALL'
                self.logger.info("Will place all blocks at a single site")

            # create rule
            RSE_EXPRESSION = 'ddm_quota>0&tier=2&rse_type=DISK'
            RSE_EXPRESSION = 'T3_IT_Trieste' # for testing
            WEIGHT = 'ddm_quota'
            WEIGHT = None # for testing
            DAYS = 14 * 24 * 3600
            ASK_APPROVAL = False
            ASK_APPROVAL = True # for testing
            copies = 1
            try:
                ruleId = self.rucioClient.add_replication_rule(dids=[containerDid],
                                                  copies=copies, rse_expression=RSE_EXPRESSION,
                                                  grouping=grouping,
                                                  weight=WEIGHT, lifetime=DAYS, account=self.username,
                                                  activity='Analysis Input',
                                                  comment='Staged from tape for %s' % self.username,
                                                  ask_approval=ASK_APPROVAL, asynchronous=True,
                                                  )
            except DuplicateRule as ex:
                # handle "A duplicate rule for this account, did, rse_expression, copies already exists"
                # which should only happen when testing, since container name is unique like task name, anyhow...
                self.logger.debug("A duplicate rule for this account, did, rse_expression, copies already exists. Use that")
                # find the existing rule id
                ruleId = self.rucioClient.list_did_rules(myScope, containerName)
            except Exception as ex:
                msg += "Rucio exception creating rule: %s" %  (str(ex))
                raise TaskWorkerException(msg)

            msg += "\nA disk replica has been requested to Rucio (rule ID: %s )" % str(ruleId[0])
            automaticTapeRecallIsImplemented = False
            if automaticTapeRecallIsImplemented:
                # set status to TAPERECALL
                tapeRecallStatus = 'TAPERECALL'
                configreq = {'workflow': self.taskName,
                             'taskstatus': tapeRecallStatus,
                             'ddmreqid': ruleId,
                             'subresource': 'addddmreqid',
                             }
                try:
                    tapeRecallStatusSet = self.server.post(self.restURInoAPI + '/task', data=urllib.urlencode(configreq))
                except HTTPException as hte:
                    self.logger.exception(hte)
                    msg = "HTTP Error while contacting the REST Interface %s:\n%s" % (
                        self.config.TaskWorker.restHost, str(hte))
                    msg += "\nSetting %s status and DDM request ID (%d) failed for task %s" % (
                        tapeRecallStatus, ddmReqId, self.taskName)
                    msg += "\nHTTP Headers are: %s" % hte.headers
                    raise TaskWorkerException(msg, retry=True)
                if tapeRecallStatusSet[2] == "OK":
                    self.logger.info("Status for task %s set to '%s'", self.taskName, tapeRecallStatus)
                    msg += "\nThis task will be automatically submitted as soon as the stage-out is completed."
                    self.uploadWarning(msg, self.userproxy, self.taskName)
                    raise TapeDatasetException(msg)
            # fall here if could not setup for automatic submission after recall
            msg += ", please try again in two days."
            raise TaskWorkerException(msg)

        if system == 'None':
            msg += "\nPlease, check DAS (https://cmsweb.cern.ch/das) and make sure the dataset is accessible on DISK."
            msg += '\nPlease see this CRAB FAQ for possible actions: https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ#crab_submit_fails_with_Task_coul'
            raise TaskWorkerException(msg)

        if system == 'Dynamo':
            raise NotImplementedError
            ddmServer = self.config.TaskWorker.DDMServer
            ddmRequest = None
            try:
                ddmRequest = blocksRequest(blockList, ddmServer, self.config.TaskWorker.cmscert,
                                           self.config.TaskWorker.cmskey, verbose=False)
            except HTTPException as hte:
                self.logger.exception(hte)
                msg += "\nThe automatic stage-out failed, please try again later. If the error persists contact the experts and provide this error message:"
                msg += "\nHTTP Error while contacting the DDM server %s:\n%s" % (ddmServer, str(hte))
                msg += "\nHTTP Headers are: %s" % hte.headers
                raise TaskWorkerException(msg, retry=True)

            self.logger.info("Contacted %s using %s and %s, got:\n%s", self.config.TaskWorker.DDMServer,
                             self.config.TaskWorker.cmscert, self.config.TaskWorker.cmskey, ddmRequest)
            # The query above returns a JSON with a format {"result": "OK", "message": "Copy requested", "data": [{"request_id": 18, "site": <site>, "item": [<list of blocks>], "group": "AnalysisOps", "n": 1, "status": "new", "first_request": "2018-02-26 23:57:37", "last_request": "2018-02-26 23:57:37", "request_count": 1}]}
            ddmReqId = ddmRequest["data"][0]["request_id"]
            if ddmRequest['result'] != 'OK':
                msg += "\nThe disk replica request failed with this error:\n %s" % ddmRequest["message"]
                raise TaskWorkerException(msg)
            msg += "\nA disk replica has been requested on %s to Dynamo (request ID: %s)" % \
                   (ddmRequest["data"][0]["first_request"], ddmReqId)
            # set status to TAPERECALL
            tapeRecallStatus = 'TAPERECALL'
            ddmReqId = ddmRequest["data"][0]["request_id"]
            configreq = {'workflow': self.taskName,
                         'taskstatus': tapeRecallStatus,
                         'ddmreqid': ddmReqId,
                         'subresource': 'addddmreqid',
                        }
            try:
                tapeRecallStatusSet = self.server.post(self.restURInoAPI + '/task', data=urllib.urlencode(configreq))
            except HTTPException as hte:
                self.logger.exception(hte)
                msg = "HTTP Error while contacting the REST Interface %s:\n%s" % (
                    self.config.TaskWorker.restHost, str(hte))
                msg += "\nSetting %s status and DDM request ID (%d) failed for task %s" % (
                    tapeRecallStatus, ddmReqId, self.taskName)
                msg += "\nHTTP Headers are: %s" % hte.headers
                raise TaskWorkerException(msg, retry=True)
            if tapeRecallStatusSet[2] == "OK":
                self.logger.info("Status for task %s set to '%s'", self.taskName, tapeRecallStatus)
                msg += "\nThis task will be automatically submitted as soon as the stage-out is completed."
                self.uploadWarning(msg, self.userproxy, self.taskName)
                raise TapeDatasetException(msg)
            else:
                msg += ", please try again in two days."
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

        self.taskName = kwargs['task']['tm_taskname']           # pylint: disable=W0201
        self.username = kwargs['task']['tm_username']           # pylint: disable=W0201
        self.userproxy = kwargs['task']['user_proxy']           # pylint: disable=W0201
        self.logger.debug("Data discovery through %s for %s", self.dbs, self.taskName)

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

        useRucioForLocations = not isUserDataset
        locationsFoundWithRucio = False

        if not useRucioForLocations:
            self.logger.info("Will not use Rucio for this dataset")

        if useRucioForLocations:
            scope = "cms"
            # If the dataset is a USER one, use the Rucio user scope to find it
            # TODO: we need a way to enable users to indicate others user scopes as source
            if isUserDataset:
                scope = "user.%s" % self.username
            self.logger.info("Looking up data location with Rucio in %s scope.", scope)
            locationsMap = {}
            try:
                for blockName in list(blocks):
                    replicas = set()
                    response = self.rucioClient.list_dataset_replicas(scope, blockName)
                    for item in response:
                        # same as complete='y' used for PhEDEx
                        if item['state'].upper() == 'AVAILABLE':
                            replicas.add(item['rse'])
                    if replicas:  # only fill map for blocks which have at least one location
                        locationsMap[blockName] = replicas
            except Exception as exc:
                msg = "Rucio lookup failed with\n%s" % str(exc)
                self.logger.warn(msg)
                locationsMap = None

            if locationsMap:
                locationsFoundWithRucio = True
            else:
                msg = "No locations found with Rucio for this dataset"
                self.logger.warn(msg)

        if not locationsFoundWithRucio:
            self.logger.info("No locations found with Rucio for %s", inputDataset)
            if isUserDataset:
                self.logger.info("USER dataset. Looking up data locations using origin site in DBS")
                try:
                    locationsMap = self.dbs.listFileBlockLocation(list(blocks))
                except Exception as ex:
                    raise TaskWorkerException(
                        "CRAB server could not get file locations from DBS for a USER dataset.\n"+\
                        "This is could be a temporary DBS glitch, please try to submit a new task (resubmit will not work)"+\
                        " and contact the experts if the error persists.\nError reason: %s" % str(ex)
                    )
            else:
                # datasets other than USER *must* be in Rucio
                raise TaskWorkerException(
                    "CRAB server could not get file locations from Rucio.\n" + \
                    "This is could be a temporary Rucio glitch, please try to submit a new task (resubmit will not work)" + \
                    " and contact the experts if the error persists."
                    )

        if secondaryDataset:
            if secondaryDataset.endswith('USER'):
                self.logger.info("Secondary dataset is USER. Looking up data locations using origin site in DBS")
                try:
                    secondaryLocationsMap = self.dbs.listFileBlockLocation(list(secondaryBlocks))
                except Exception as ex:
                    raise TaskWorkerException(
                        "CRAB server could not get file locations from DBS for secondary dataset of USER tier.\n"+\
                        "This is could be a temporary DBS glitch, please try to submit a new task (resubmit will not work)"+\
                        " and contact the experts if the error persists.\nError reason: %s" % str(ex)
                    )
            else:
                self.logger.info("Trying data location of secondary dataset blocks with Rucio")
                secondaryLocationsMap = {}
                try:
                    for blockName in list(secondaryBlocks):
                        replicas = set()
                        response = self.rucioClient.list_dataset_replicas(scope, blockName)
                        for item in response:
                            # same as complete='y' used for PhEDEx
                            if item['state'].upper() == 'AVAILABLE':
                                replicas.add(item['rse'])
                        if replicas:  # only fill map for blocks which have at least one location
                            secondaryLocationsMap[blockName] = replicas
                except Exception as exc:
                    msg = "Rucio lookup failed with\n%s" % str(exc)
                    self.logger.warn(msg)
                    secondaryLocationsMap = None
            if not secondaryLocationsMap:
                msg = "No locations found for secondaryDataset %s." % secondaryDataset
                raise TaskWorkerException(msg)


        # From now on code is not dependent from having used Rucio or PhEDEx

        blocksWithLocation = locationsMap.keys()
        if secondaryDataset:
            secondaryBlocksWithLocation = secondaryLocationsMap.keys()

        # filter out TAPE locations
        self.keepOnlyDiskRSEs(locationsMap)
        if not locationsMap:
            msg = "Task could not be submitted because there is no DISK replica for dataset %s" % inputDataset
            if self.tapeLocations:
                msg += "\nN.B.: the input dataset is stored at %s, but those are TAPE locations." % ', '.join(sorted(self.tapeLocations))
                # following function will always raise error and stop flow here, but will first
                # try to trigger a tape recall and place the task in tapeRecall status
                self.requestTapeRecall(blockList=blocksWithLocation, system='Rucio', msgHead=msg)

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
        if not filedetails:
            raise TaskWorkerException(("Cannot find any file inside the dataset. Please, check your dataset in DAS, %s.\n" +\
                                "Aborting submission. Resubmitting your task will not help.") %\
                                ("https://cmsweb.cern.ch/das/request?instance=%s&input=dataset=%s") %\
                                (self.dbsInstance, inputDataset))

        ## Format the output creating the data structures required by WMCore. Filters out invalid files,
        ## files whose block has no location, and figures out the PSN
        result = self.formatOutput(task=kwargs['task'], requestname=self.taskName,
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
    ###
    # Usage: python DBSDataDiscovery.py dbs_instance dbsDataset [secondaryDataset]
    # where dbs_instance should be either prod/global or prod/phys03
    # Needs to define first:
    # export X509_USER_CERT=/data/certs/servicecert.pem
    # export X509_USER_KEY=/data/certs/servicekey.pem
    #
    # Example: python ~/repos/CRABServer/src/python/TaskWorker/Actions/DBSDataDiscovery.py prod/global /MuonEG/Run2016B-23Sep2016-v3/MINIAOD
    ###
    dbsInstance = sys.argv[1]
    dbsDataset = sys.argv[2]
    dbsSecondaryDataset = sys.argv[3] if len(sys.argv) == 4 else None
    DBSUrl = 'https://cmsweb.cern.ch/dbs/%s/DBSReader/' % dbsInstance

    logging.basicConfig(level=logging.DEBUG)
    from WMCore.Configuration import ConfigurationEx
    from ServerUtilities import newX509env
    from RucioUtils import getNativeRucioClient

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

    #config.TaskWorker.DDMServer = 'dynamo.mit.edu'
    config.TaskWorker.instance = 'prod'
    #config.TaskWorker.restHost = 'cmsweb.cern.ch'
    # The second word identifies the DB instance defined in CRABServerAuth.py on the REST
    #config.TaskWorker.restURInoAPI = '/crabserver/prod/'

    config.Services.Rucio_host = 'https://cms-rucio.cern.ch'
    config.Services.Rucio_account = 'crab_server'
    config.Services.Rucio_authUrl = 'https://cms-rucio-auth.cern.ch'
    config.Services.Rucio_caPath = '/etc/grid-security/certificates/'
    rucioClient = getNativeRucioClient(config=config, logger=logging.getLogger())

    fileset = DBSDataDiscovery(config=config, rucioClient=rucioClient)
    fileset.execute(task={'tm_nonvalid_input_dataset': 'T', 'tm_use_parent': 0, 'user_proxy': 'None',
                          'tm_input_dataset': dbsDataset, 'tm_secondary_input_dataset': dbsSecondaryDataset,
                          'tm_taskname': 'pippo1', 'tm_username':config.Services.Rucio_account,
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
#    dataset = '/GenericTTbar/belforte-Stefano-Test-bb695911428445ed11a1006c9940df69/USER' # USER dataset on prod/phys03
#===============================================================================
