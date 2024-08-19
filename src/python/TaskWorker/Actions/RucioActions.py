"""
a Class to perform Actions useful to TaskWorker via Rucio
"""
import random
import copy
from urllib.parse import urlencode
from http.client import HTTPException

from RucioUtils import getNativeRucioClient
from rucio.common.exception import (DuplicateRule, DataIdentifierAlreadyExists, DuplicateContent,
                                    InsufficientTargetRSEs, InsufficientAccountLimit, FullStorage)

from ServerUtilities import MAX_DAYS_FOR_TAPERECALL, MAX_TB_TO_RECALL_AT_A_SINGLE_SITE
from ServerUtilities import TASKLIFETIME
from TaskWorker.WorkerUtilities import uploadWarning
from TaskWorker.WorkerExceptions import TaskWorkerException, TapeDatasetException, SubmissionRefusedException


class RucioAction():
    """
    Instantiates an object to performs action on tasks in TW context
    which require non trivial interaction with Rucio.
    Allows for different actions to use different Rucio accounts
    """
    def __init__(self, config=None, crabserver=None, rucioAcccount=None,
                 taskName=None, username=None, logger=None):
        self.config = config
        self.crabserver = crabserver
        self.taskName = taskName
        self.logger = logger
        self.username = username
        self.rucioAccount = rucioAcccount
        myConfig = copy.deepcopy(self.config)
        myConfig.Services.Rucio_account = self.rucioAccount
        self.rucioClient = getNativeRucioClient(myConfig, self.logger)  # pylint: disable=redefined-outer-name

    def makeContainerFromBlockList(self, blockList=None, containerDid=None):
        """ create container and fill with given blocks """
        scope = containerDid['scope']
        containerName = containerDid['name']
        # turn input CMS blocks into Rucio dids in cms scope
        dids = [{'scope': 'cms', 'name': block} for block in blockList]
        # prepare container
        self.logger.info("Create Rucio container %s", containerName)
        try:
            self.rucioClient.add_container(scope, containerName)
        except DataIdentifierAlreadyExists:
            self.logger.debug("Container name already exists in Rucio. Keep going")
        except Exception as e:
            msg = f"\nRucio exception creating container: {e}"
            raise TaskWorkerException(msg) from e
        # add block dids to container
        try:
            self.rucioClient.attach_dids(scope, containerName, dids)
        except DuplicateContent:
            self.logger.debug("Some dids are already in this container. Keep going")
        except Exception as e:
            msg = f"\nRucio exception adding blocks to container: {e}"
            raise TaskWorkerException(msg) from e
        self.logger.info("Rucio container %s:%s created with %d blocks", scope, containerName, len(blockList))

    def createOrReuseRucioRule(self, did=None, grouping=None, activity=None,
                               rseExpression='', comment='', lifetime=0):
        """ if Rucio reports duplicate rule exception, reuse existing one """
        # Some RSE_EXPR for testing
        # rseExpression = 'ddm_quota>0&(tier=1|tier=2)&rse_type=DISK'
        # rseExpression = 'T3_IT_Trieste' # for testing
        weight = 'ddm_quota'  # only makes sense for rules which trigger replicas
        # weight = None # for testing
        if activity == "Analysis TapeRecall":
            askApproval = True
            account = self.username
        else:
            askApproval = False
            account = self.rucioAccount
        # askApproval = True # for testing
        copies = 1
        try:
            ruleIds = self.rucioClient.add_replication_rule(  # N.B. returns a list
                dids=[did], copies=copies, rse_expression=rseExpression,
                grouping=grouping, weight=weight, lifetime=lifetime,
                account=account, activity=activity,
                comment=comment,
                ask_approval=askApproval, asynchronous=True)
            ruleId = ruleIds[0]
        except DuplicateRule:
            # handle "A duplicate rule for this account, did, rse_expression, copies already exists"
            # which should only happen when testing, since container name is unique like task name, but
            # we cover also retry situations where TW was stopped/killed/crashed halfway in the process
            self.logger.debug("A duplicate rule for this account, did, rse_expression, copies already exists. Use that")
            # find the existing rule id
            ruleIdGen = self.rucioClient.list_did_rules(scope=did['scope'], name=did['name'])
            self.logger.debug("List of existing rules for this DID")
            ruleId = None
            for rule in ruleIdGen:
                self.logger.debug("id: %s account: %s activity %s")
                if rule['account'] == account:
                    ruleId = rule['id']
                    break
            if not ruleId:
                msg = "Failed to creaed Rucio rule to recall data. Rucio DuplicateException raised "
                msg += "but a rule for this account was not found in the list"
                raise TaskWorkerException(msg) from DuplicateRule
            # extend rule lifetime
            self.rucioClient.update_replication_rule(ruleId, {'lifetime': lifetime})
        except (InsufficientTargetRSEs, InsufficientAccountLimit, FullStorage) as e:
            msg = f"\nNot enough global quota to issue a tape recall request. Rucio exception:\n{e}"
            raise TaskWorkerException(msg) from e
        except Exception as e:
            msg = f"\nRucio exception creating rule: {e}"
            raise TaskWorkerException(msg) from e

        return ruleId

    def whereToRecall(self, tapeLocations=None, teraBytesToRecall=0):
        """ decide which RSE's to use """
        # make RSEs lists
        # asking for ddm_quota>0 gets rid also of Temp and Test RSE's
        allRses = "ddm_quota>0&(tier=1|tier=2)&rse_type=DISK"
        # tune list according to where tapes are, we expect a single location
        # if there are more... let Rucio deal with them to find best route
        if tapeLocations:
            if 'T1_RU_JINR_Tape' in tapeLocations and len(tapeLocations) > 1:
                tapeLocations.remove('T1_RU_JINR_Tape')  # JINR tape data are often duplicated
            if len(tapeLocations) == 1:
                # do not recall across the Atlantic
                if 'US' in list(tapeLocations)[0]:
                    allRses += "&(country=US)"  # Brasil is on that side too, but not reliable enough
                else:
                    # Rucio wants the set complement operator \
                    allRses += r"\country=US\country=BR"  # pylint: disable=anomalous-backslash-in-string
                    # avoid fragile sites, see #7400
                    allRses += r"\country=RU\country=UA"  # pylint: disable=anomalous-backslash-in-string
        rses = self.rucioClient.list_rses(allRses)
        rseNames = [r['rse'] for r in rses]
        largeRSEs = []  # a list of largish (i.e. solid) RSEs
        for rse in rseNames:
            # avoid large but not-very-reliable-yet sites
            if rse in ['T2_IN_TIFR', 'T2_PL_Swierk']:
                continue
            usageDetails = list(self.rucioClient.get_rse_usage(rse, filters={'source': 'static'}))
            if not usageDetails:  # some RSE's are put in the system with an empty list here, e.g. T2_FR_GRIF
                continue
            size = usageDetails[0]['used']  # bytes
            if float(size) / 1.e15 > 1.0:  # more than 1 PB
                largeRSEs.append(rse)
        # sort lists so that duplicated rules can be spotted
        largeRSEs.sort()
        if teraBytesToRecall <= MAX_TB_TO_RECALL_AT_A_SINGLE_SITE:  #
            grouping = 'ALL'
            self.logger.info("Will place all blocks at a single site")
            rseExpression = '|'.join(largeRSEs)  # any solid site will do, most datasets are a few TB anyhow
        else:
            grouping = 'DATASET'  # Rucio DATASET i.e. CMS block !
            self.logger.info("Will scatter blocks on multiple sites")
            # restrict the list to as few sites as possible
            nRSEs = int(teraBytesToRecall / MAX_TB_TO_RECALL_AT_A_SINGLE_SITE) + 1
            myRSEs = random.sample(largeRSEs, nRSEs)
            rseExpression = '|'.join(myRSEs)
        self.logger.debug('Will use rseExpression = %s', rseExpression)
        return rseExpression, grouping

    def setTaskToTapeRecall(self, ruleId='', msg=''):
        """ set task status in TASK table of CRAB DB """
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
            msg = f"HTTP Error while contacting the REST Interface {self.crabserver.server['host']}:\n{hte}"
            msg += f"\nStoring of {tapeRecallStatus} status and ruleId ({ruleId}) failed for task {self.taskName}"
            msg += f"\nHTTP Headers are: {hte.headers}"
            raise TaskWorkerException(msg, retry=True) from hte
        if tapeRecallStatusSet[2] == "OK":
            self.logger.info("Status for task %s set to '%s'", self.taskName, tapeRecallStatus)
        msg += "\nThis task will be automatically submitted as soon as the stage-out is completed."
        uploadWarning(warning=msg, taskname=self.taskName, crabserver=self.crabserver, logger=self.logger)
        raise TapeDatasetException(msg)

    def recallData(self, dataToRecall=None, sizeToRecall=0, tapeLocations=None, msgHead=''):

        """
        implements tape recall with Rucio
        params and returns are the same as requestTapeRecall defined above
        """

        # friendly units from bytes to TB
        teraBytesToRecall = sizeToRecall // 1e12
        # Sanity check
        if teraBytesToRecall > 1e3:
            msg = msgHead + f"\nDataset size {teraBytesToRecall} TB. Will not trigger automatic recall for >1PB. Contact DataOps"
            raise SubmissionRefusedException(msg)

        # a friendly-formatted string to print the size
        recallSize = f"{teraBytesToRecall:.0f} TBytes" if teraBytesToRecall > 0 else f"{sizeToRecall / 1e9:.0f} GBytes"
        self.logger.info("Total size of data to recall : %s", recallSize)

        # prepare container to be recalled
        if isinstance(dataToRecall, str):
            # recalling a full DBS dataset. Simple: the container already exists
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
            containerName = f"/TapeRecall/{self.taskName.replace(':', '.')}/USER"
            containerDid = {'scope': myScope, 'name': containerName}
            self.makeContainerFromBlockList(blockList=dataToRecall, containerDid=containerDid)
            # beware blockList being not subscriptable (e.g. dict_keys type)
            dbsDatasetName = next(iter(dataToRecall)).split('#')[0]
        else:
            return

        # prepare comment to be inserted in the Rucio rule
        comment = f"Recall {recallSize} for user: {self.username} dataset: {dbsDatasetName}"

        # define where to recall to, which depends also on where data are on tape
        (rseExpression, grouping) = self.whereToRecall(tapeLocations=tapeLocations, teraBytesToRecall=teraBytesToRecall)

        # create rule
        # make rule last 7 extra days to allow debugging in case TW or Recall action fail
        lifetime = (MAX_DAYS_FOR_TAPERECALL + 7) * 24 * 60 * 60  # in seconds
        ruleId = self.createOrReuseRucioRule(did=containerDid, grouping=grouping,
                                             rseExpression=rseExpression,
                                             activity="Analysis TapeRecall",
                                             comment=comment, lifetime=lifetime)
        msg = f"Created Rucio rule ID: {ruleId}"
        self.logger.info(msg)

        # this will be printed by "crab status"
        msg = msgHead
        msg += "\nA disk replica has been requested to Rucio.  Check progress via:"
        msg += f"\n  rucio rule-info {ruleId}"
        msg += f"\nor simply check 'state' line in this page: https://cms-rucio-webui.cern.ch/rule?rule_id={ruleId}"

        # update task status in CRAB DB and store the rule ID
        self.setTaskToTapeRecall(ruleId=ruleId, msg=msg)

        return

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
            containerName = f"/TapeRecall/{self.taskName.replace(':', '.')}/USER"
            containerDid = {'scope': myScope, 'name': containerName}
            self.makeContainerFromBlockList(blockList=dataToLock, containerDid=containerDid)
            # beware blockList being not subscriptable (e.g. dict_keys type)
            dbsDatasetName = next(iter(dataToLock)).split('#')[0]
        else:
            return

        # prepare comment to be inserted in the Rucio rule
        partOf = "" if not isinstance(dataToLock, list) else "part of"
        comment = f"Lock {partOf} dataset: {dbsDatasetName} for user: {self.username}"

        # create rule
        ruleId = self.createOrReuseRucioRule(did=containerDid, rseExpression='rse_type=DISK',
                                             activity="Analysis Input",
                                             comment=comment, lifetime=TASKLIFETIME)
        msg = f"Created Rucio rule ID: {ruleId}"
        self.logger.info(msg)
        return
