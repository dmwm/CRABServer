import os
import random
import copy
from urllib.parse import urlencode
from http.client import HTTPException


from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.WorkerExceptions import TaskWorkerException, TapeDatasetException

from ServerUtilities import FEEDBACKMAIL, MAX_DAYS_FOR_TAPERECALL, MAX_TB_TO_RECALL_AT_A_SINGLE_SITE
from ServerUtilities import TASKLIFETIME

from RucioUtils import getNativeRucioClient

from rucio.common.exception import (DuplicateRule, DataIdentifierAlreadyExists, DuplicateContent,
                                    InsufficientTargetRSEs, InsufficientAccountLimit, FullStorage)

class RucioActions():
    """
    performs action on tasks which require non trivial interaction with Rucio
    contains a few common methods to the two classes tapeRecaller and inputLocker
    acting as base class for those
    """
    def __init__(self, config=None, crabserver=None, taskName=None, username=None, logger=None):
        self.config = config
        self.crabserver = crabserver
        self.taskName = taskName
        self.logger = logger
        self.username = username
        # following two will be filled in inherited subclasses TapeRecaller and InputLocker
        self.rucioAccount = None
        self.rucioClient = None


    def makeContainerFromBlockList(self, rucio=None, blockList=None, containerDid=None):
        """ create container and fill with given blocks """
        scope = containerDid['scope']
        containerName = containerDid['name']
        # turn input CMS blocks into Rucio dids in cms scope
        dids = [{'scope': 'cms', 'name': block} for block in blockList]
        # prepare container
        self.logger.info("Create Rucio container %s", containerName)
        try:
            rucio.add_container(scope, containerName)
        except DataIdentifierAlreadyExists:
            self.logger.debug("Container name already exists in Rucio. Keep going")
        except Exception as ex:
            msg = "\nRucio exception creating container: %s" % (str(ex))
            raise TaskWorkerException(msg) from ex
        # add block dids to container
        try:
            rucio.attach_dids(scope, containerName, dids)
        except DuplicateContent:
            self.logger.debug("Some dids are already in this container. Keep going")
        except Exception as ex:
            msg = "\nRucio exception adding blocks to container: %s" % (str(ex))
            raise TaskWorkerException(msg) from ex
        self.logger.info("Rucio container %s:%s created with %d blocks", scope, containerName, len(blockList))


    def createOrReuseRucioRule(self, did=None, grouping=None,
                               RSE_EXPRESSION='', comment='', lifetime=0):
        # Some RSE_EXPR for testing
        # RSE_EXPRESSION = 'ddm_quota>0&(tier=1|tier=2)&rse_type=DISK'
        # RSE_EXPRESSION = 'T3_IT_Trieste' # for testing
        WEIGHT = 'ddm_quota'  # only makes sense for rules which trigger replicas
        # WEIGHT = None # for testing
        ASK_APPROVAL = False
        # ASK_APPROVAL = True # for testing
        copies = 1
        try:
            ruleIds = self.rucioClient.add_replication_rule(  # N.B. returns a list
                dids=[did], copies=copies, rse_expression=RSE_EXPRESSION,
                grouping=grouping, weight=WEIGHT, lifetime=lifetime,
                account=self.rucioAccount, activity='Analysis Input',
                comment=comment,
                ask_approval=ASK_APPROVAL, asynchronous=True)
            ruleId = ruleIds[0]
        except DuplicateRule:
            # handle "A duplicate rule for this account, did, rse_expression, copies already exists"
            # which should only happen when testing, since container name is unique like task name, but
            # we cover also retry situations where TW was stopped/killed/crashed halfway in the process
            self.logger.debug("A duplicate rule for this account, did, rse_expression, copies already exists. Use that")
            # find the existing rule id
            ruleIdGen = self.rucioClient.list_did_rules(scope=did['scope'], name=did['name'])
            for rule in ruleIdGen:
                if rule['account'] == self.rucioAccount:
                    ruleId = rule['id']
                    break
            # extend rule lifetime
            self.rucioClient.update_replication_rule(ruleId, {'lifetime': lifetime})
        except (InsufficientTargetRSEs, InsufficientAccountLimit, FullStorage) as ex:
            msg = "\nNot enough global quota to issue a tape recall request. Rucio exception:\n%s" % str(ex)
            raise TaskWorkerException(msg) from ex
        except Exception as ex:
            msg = "\nRucio exception creating rule: %s" % str(ex)
            raise TaskWorkerException(msg) from ex

        return ruleId



class TapeRecaller(RucioActions):
    """
    request tape recall
    """
    def __init__(self, config=None, crabserver=None, taskName=None, username=None, logger=None):
        RucioActions.__init__(self, config=config, crabserver=crabserver, taskName=taskName,
                       username=username, logger=logger)
        # need to use crab_tape_recall Rucio account to create containers and create rules
        self.rucioAccount = 'crab_tape_recall'
        tapeRecallConfig = copy.deepcopy(self.config)
        tapeRecallConfig.Services.Rucio_account = self.rucioAccount
        self.rucioClient = getNativeRucioClient(tapeRecallConfig, self.logger)  # pylint: disable=redefined-outer-name


    def whereToRecall(self, tapeLocations=None, TBtoRecall=0):
        # make RSEs lists
        # asking for ddm_quota>0 gets rid also of Temp and Test RSE's
        ALL_RSES = "ddm_quota>0&(tier=1|tier=2)&rse_type=DISK"
        # tune list according to where tapes are, we expect a single location
        # if there are more... let Rucio deal with them to find best route
        if tapeLocations:
            if 'T1_RU_JINR_Tape' in tapeLocations and len(tapeLocations) > 1:
                tapeLocations.remove('T1_RU_JINR_Tape')  # JINR tape data are often duplicated
            if len(tapeLocations) == 1:
                if 'US' in list(tapeLocations)[0]:
                    ALL_RSES += "&(country=US|country=BR)"
                else:
                    ALL_RSES += "\country=US\country=BR"  # Rucio wants the set complement operator \
        rses = self.rucioClient.list_rses(ALL_RSES)
        rseNames = [r['rse'] for r in rses]
        largeRSEs = []  # a list of largish (i.e. solid) RSEs
        for rse in rseNames:
            if rse[2:6] == '_RU_':  # avoid fragile sites, see #7400
                continue
            usageDetails = list(self.rucioClient.get_rse_usage(rse, filters={'source': 'static'}))
            if not usageDetails:  # some RSE's are put in the system with an empty list here, e.g. T2_FR_GRIF
                continue
            size = usageDetails[0]['used']  # bytes
            if float(size) / 1.e15 > 1.0:  # more than 1 PB
                largeRSEs.append(rse)
        # sort lists so that duplicated rules can be spotted
        largeRSEs.sort()
        if TBtoRecall <= MAX_TB_TO_RECALL_AT_A_SINGLE_SITE:  #
            grouping = 'ALL'
            self.logger.info("Will place all blocks at a single site")
            RSE_EXPRESSION = '|'.join(largeRSEs)  # any solid site will do, most datasets are a few TB anyhow
        else:
            grouping = 'DATASET'  # Rucio DATASET i.e. CMS block !
            self.logger.info("Will scatter blocks on multiple sites")
            # restrict the list to as few sites as possible
            nRSEs = int(TBtoRecall / MAX_TB_TO_RECALL_AT_A_SINGLE_SITE) + 1
            myRSEs = random.sample(largeRSEs, nRSEs)
            RSE_EXPRESSION = '|'.join(myRSEs)
        self.logger.debug('Will use RSE_EXPRESSION = %s', RSE_EXPRESSION)
        return RSE_EXPRESSION, grouping


    def setTaskToTapeRecall(self, ruleId='', msg=''):
        tapeRecallStatus = 'TAPERECALL'
        configreq = {'workflow': self.taskName,
                     'taskstatus': tapeRecallStatus,
                     'ddmreqid': ruleId,
                     'subresource': 'addddmreqid',
                     }
        try:
            tapeRecallStatusSet = self.crabserver.post(api='task', data=urlencode(configreq))
        except HTTPException as hte:
            self.logger.exception(hte)
            msg = "HTTP Error while contacting the REST Interface %s:\n%s" % (
                self.crabserver.server['host'], str(hte))
            msg += "\nStoring of %s status and ruleId (%s) failed for task %s" % (
                tapeRecallStatus, ruleId, self.taskName)
            msg += "\nHTTP Headers are: %s" % hte.headers
            raise TaskWorkerException(msg, retry=True) from hte
        if tapeRecallStatusSet[2] == "OK":
            self.logger.info("Status for task %s set to '%s'", self.taskName, tapeRecallStatus)
        msg += "\nThis task will be automatically submitted as soon as the stage-out is completed."
        self.uploadWarning(msg, self.userproxy, self.taskName)
        raise TapeDatasetException(msg)


    def recallData(self, dataToRecall=None, sizeToRecall=0, tapeLocations=None, msgHead=''):

        """
        implements tape recall with Rucio
        params and returns are the same as requestTapeRecall defined above
        """

        # friendly units from bytes to TB
        TBtoRecall = sizeToRecall // 1e12
        # Sanity check
        if TBtoRecall > 1e3:
            msg = msgHead + f"\nDataset size {TBtoRecall} TB. Will not trigger automatic recall for >1PB. Contact DataOps"
            raise TaskWorkerException(msg)

        # a friendly-formatted string to print the size
        recallSize = f"{TBtoRecall:.0f} TBytes" if TBtoRecall > 0 else f"{sizeToRecall / 1e9:.0f} GBytes"
        self.logger.info("Total size of data to recall : %s", recallSize)


        # prepare container to be recalled
        if isinstance(dataToRecall, str):
            # recalling a full DBS dataset, simple the container already exists
            myScope = 'cms'
            dbsDatasetName = dataToRecall
            containerDid = {'scope': myScope, 'name': dbsDatasetName}
        elif isinstance(dataToRecall, list):
            # need to prepare ad hoc container. Can not reuse a possibly existing one:
            # if a user wants one block from a large dataset, we must
            # not recall also all blocks possibly requested in the past
            #  by other users for other reasons.
            #  Therefore we stick with naming the container after the task name
            myScope = f"user.{self.rucioAccount}"  # do not mess with cms scope
            containerName = '/TapeRecall/%s/USER' % self.taskName.replace(':', '.')
            containerDid = {'scope': myScope, 'name': containerName}
            self.makeContainerFromBlockList(blockList=dataToRecall, containerDid=containerDid)
            # beware blockList being not subscriptable (e.g. dict_keys type)
            dbsDatasetName = next(iter(dataToRecall)).split('#')[0]
        else:
            return

        # prepare comment to be inserted in the Rucio rule
        comment = f"Recall {recallSize} for user: {self.username} dataset: {dbsDatasetName}"

        # define where to recall to, which depends also on where data are on tape
        (RSE_EXPRESSION, grouping) = self.whereToRecall(tapeLocations=tapeLocations, TBtoRecall=TBtoRecall)

        # create rule
        # make rule last 7 extra days to allow debugging in case TW or Recall action fail
        LIFETIME = (MAX_DAYS_FOR_TAPERECALL + 7) * 24 * 60 * 60  # in seconds
        ruleId = self.createOrReuseRucioRule(did=containerDid, grouping=grouping,
                                             RSE_EXPRESSION=RSE_EXPRESSION,
                                             comment=comment, lifetime=LIFETIME)
        msg = f"Created Rucio rule ID: {ruleId}"
        self.logger.info(msg)

        # this will be printed by "crab status"
        msg = msgHead
        msg += "\nA disk replica has been requested to Rucio.  Check progress via:"
        msg += "\n  rucio rule-info %s" % ruleId
        msg += "\nor simply check 'state' line in this page: https://cms-rucio-webui.cern.ch/rule?rule_id=%s" % ruleId

        # update task status in CRAB DB and store the rule ID
        self.setTaskToTapeRecall(ruleId=ruleId, msg=msg)

        return


class InputLocker(RucioActions):
    """
    lock input data for the lifetime of the task
    """
    def __init__(self, config=None, crabserver=None, taskName=None, username=None, logger=None):
        RucioActions.__init__(self, config=config, crabserver=crabserver, taskName=taskName,
                       username=username, logger=logger)
        # need to use crab_input Rucio account to create containers and create rules
        self.rucioAccount = 'crab_input'
        tapeRecallConfig = copy.deepcopy(self.config)
        tapeRecallConfig.Services.Rucio_account = self.rucioAccount
        self.rucioClient = getNativeRucioClient(tapeRecallConfig, self.logger)  # pylint: disable=redefined-outer-name


    def lockData(self, dataToLock=None):

        """
        lock input data on disk via a Rucio rule
        """
        if not isinstance(dataToLock, str) and not isinstance(dataToLock, list):
            return

        msg = f"Lock {dataToLock} on disk"
        self.logger.info(msg)

        # prepare container to be locked
        if isinstance(dataToLock, str):
            # recalling a full DBS dataset, simple the container already exists
            myScope = 'cms'
            dbsDatasetName = dataToLock
            containerDid = {'scope': myScope, 'name': dbsDatasetName}
        elif isinstance(dataToLock, list):
            # need to prepare ad hoc container. Can not reuse a possibly existing one
            # for this dataset which may have had a different content
            # Therefore we stick with naming the container after the task name
            myScope = f"user.{self.rucioAccount}"  # do not mess with cms scope
            containerName = '/TapeRecall/%s/USER' % self.taskName.replace(':', '.')
            containerDid = {'scope': myScope, 'name': containerName}
            self.makeContainerFromBlockList(
                rucio=self.rucioClient, blockList=dataToLock,
                containerDid=containerDid)
            # beware blockList being not subscriptable (e.g. dict_keys type)
            dbsDatasetName = next(iter(dataToLock)).split('#')[0]
        else:
            return

        # prepare comment to be inserted in the Rucio rule
        partOf = "" if not isinstance(dataToLock, list) else "part of"
        comment = f"Lock {partOf} dataset: {dbsDatasetName} for user: {self.username}"

        # create rule
        ruleId = self.createOrReuseRucioRule(did=containerDid, RSE_EXPRESSION='rse_type=DISK',
                                             comment=comment, lifetime=TASKLIFETIME)
        msg = f"Created Rucio rule ID: {ruleId}"
        self.logger.info(msg)
        return
