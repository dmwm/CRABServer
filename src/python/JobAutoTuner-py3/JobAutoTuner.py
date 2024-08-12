#!/usr/bin/python3

"""
This Script is intended to load separately the different scripts related
to job modifications done through the  condor JobRouter in the CRAB3 SChedd.
it is done in the following order:
1. JobTimeTuner.py
2. CMSLPCRoute.py
"""

import logging
import time
import warnings
import sys
from datetime import datetime
from pprint import pformat
from distutils.util import strtobool

import htcondor
import classad


class GwmsmonAlarm():
    """
    An Alarm raised if gwmsmon.cern.ch returned an empty list of jobs/tasks.
    (this alarm is an 'always' executed warning which can easyly be converted to a real exception by
    setting this to 'error' in warnings.filterwarnings())

    Attributes:
    message -- explanation of the error
    """
    # NB here to send emails to the operator or lemon alarms
    def __init__(self, message):
        self.message = message
        self.time = str(datetime.now())
        alarmMessage = ("WARNING: GwmsmonAlarm: at: %s / additional info: %s" % (self.time, self.message))
        logging.warning(alarmMessage)


class ScheddAlarm():
    """
    An Alarm raised if we couldn't make `condor_q` for any reason.
    (this alarm is an 'error' warning which is a real exception this could be  changed by
    setting this to 'always/default/once/ignore/module' in warnings.filterwarnings())

    Attributes:
    message -- explanation of the error
    """
    # NB here to send emails to the operator or lemon alarms
    def __init__(self, message):
        self.message = message
        self.time = str(datetime.now())
        alarmMessage = ("ERR: Could not query the schedd at: %s / additional info: %s" % (self.time,self.message))
        logging.error(alarmMessage)


class ConfigAlarm():
    """
    An Alarm raised if we couldn't find the config parameters in htcondor for any reason.

    Attributes:
    configKey -- config paramater that is searched in condor_config_val
    message -- additional info for the alarm
    """
    # NB here to send emails to the operator or lemon alarms
    def __init__(self, configKey, errMessage ):
        self.configKey = configKey
        self.errMessage = errMessage
        alarmMessage = ("ERR: Could not find/read the config parameter: %s. Not enabling the service! Additional info: %s" % (self.configKey, self.errMessage))
        logging.error(alarmMessage)


class SubScriptAlarm():
    """
    An Alarm raised if one of the subscripts exit with a nonZero status.
    """
    def __init__(self, message, exc=None):
        self.message = message
        self.exc = exc
        self.time = str(datetime.now())
        alarmMessage = ("ERR: SubScriptAlarm: at: %s / additional info: \n%s\nException: %s" % (self.time, self.message,self.exc))
        logging.exception(alarmMessage)


def readCondorFlag(condorFlag):
    """ get a True/Flase boolean from condor config """
    returnFlag = False
    if condorFlag not in htcondor.param.keys():
        alarm=ConfigAlarm(configKey=condorFlag, errMessage="Config key not set in condor.")
        warnings.warn(alarm)
    else:
        try:
            returnFlag = strtobool(htcondor.param.get(condorFlag))
        except Exception as e:  # pylint: disable=broad-except
            alarm=ConfigAlarm(configKey=condorFlag,errMessage=e)
            warnings.warn(alarm)
    return returnFlag


class Overflow:
    """
    The major class for the Overflow.
    """

    def __init__(self):
        self.initMessage = "=================== An Overflow object instance start! ==================="
        logging.info(self.initMessage)

        # The maximum Job Idle Time allowed beofeore an Overflow route is generated (min.)
        self.maxIdleTime = 20  # roughly four~five negotiator cycles

        # we stick to hardcoded list of site names, since we only overflow
        # from European T1's to same contry T2's and list of sites in those regions is very stable
        self.tier1s = ['T1_DE_KIT', 'T1_ES_PIC', 'T1_FR_CCIN2P3', 'T1_IT_CNAF', 'T1_UK_RAL']
        self.nearbyT2s = {
            # list sites in format for DESIRED_SITES classAd a string of comma separated workds
            'T1_DE_KIT': 'T2_DE_DESY,T2_DE_RWTH',
            'T1_ES_PIC': 'T2_ES_CIEMAT,T2_ES_IFCA',
            'T1_FR_CCIN2P3': 'T2_FR_GRIF,T2_FR_IPHC',
            'T1_IT_CNAF': 'T2_IT_Bari,T2_IT_Legnaro,T2_IT_Pisa,T2_IT_Rome',
            'T1_UK_RAL': 'T2_UK_London_Brunel,T2_UK_London_IC,T2_UK_SGrid_Bristol,T2_UK_SGrid_RALPP',
        }

        self.htCollector = htcondor.param['COLLECTOR_HOST']
        logging.debug("HTCondor collector = %s", self.htCollector)

        self.schedd = htcondor.Schedd()

        self.requirementsString = None
        self.requirementsStringSet = set()
        self.currJobObj = None
        self.currSiteListDiff = None
        self.currSiteList = None

        self.initMessage = "=================== An Overflow object instance end! ==================="
        logging.debug(self.initMessage)

    def createJobRouterAd(self, requirementsString, newSiteList, routeIdentifier):
        """
        A function used to prepare and print the routes to be added to the classad for the Jobrouter.
        This must be the only one to print something in the script's output,
        because that is how the Jobrouter is reading the new classads.
        everything else from the script must go in the relevant logging levels inside the logfile
        """

        if type(requirementsString) != str:
            logging.warning("Wrong type %s requirementsString of  %s", requirementsString.__class__, requirementsString)
            return

        routerEntry = classad.ClassAd()
        # done - to check if all the  classaads generated for the JobRouter need to have unique names:
        #        must be unique so overflowLevel is not a good  identifier

        classadName = "Overflow: %s" % routeIdentifier
        routerEntry["Name"] = classadName
        routerEntry["OverrideRoutingEntry"] = True
        routerEntry["EditJobInPlace"] = True
        routerEntry["TargetUniverse"] = 5
        routerEntry["Requirements"] = classad.ExprTree(str(requirementsString))
        routerEntry["set_DESIRED_SITES"] = newSiteList
        routerEntry["set_DESIRED_SITES_Orig"] = self.currSiteList
        routerEntry["set_DESIRED_SITES_Diff"] = self.currSiteListDiff
        routerEntry["set_HasBeenOverflowRouted"] = True
        routerEntry["set_RouteType"] = "overflow"
        routerEntry["eval_set_LastRouted"] = classad.ExprTree('time()')

        logging.info("A new Route has been added:  %s", routerEntry)
        print(routerEntry)

    def needOverflow(self, jobObject):
        """
        simply check if job has been idle for longer
        than maxIdleTime for one of the listed T1's
        returns: True/False
        """

        currIdleTime = int(time.time()) - jobObject["QDate"]
        currIdleTime = int(currIdleTime/60)  # from seconds to minutes

        idleTooMuch = currIdleTime > 0 and (currIdleTime - self.maxIdleTime) > 0
        # exit here if the above criteria was false
        if not idleTooMuch:
            return False

        logging.debug("job was idle > %s min", self.maxIdleTime)
        if len(jobObject["DESIRED_Sites"].split(',')) > 1:
            # targeting 2 or more sites
            return False

        # only one site in DESIRED_Sites
        siteName = jobObject["DESIRED_Sites"]
        if not siteName in self.tier1s:
            # we only overflow from T1's
            return False

        return True

    def overflow(self, jobsInThisSchedd):
        """ create and print overflow routes as needed """

        for _, jobObj in jobsInThisSchedd.items():
            self.currJobObj = jobObj
            # Set the self.currJobObj  var to the current job in order to extract classads from other functions
            # Set the self.currSiteListDiff var to be used later as the DESIRED_SITES_Diff classad (
            # This one cannot be added as a field to the self.currJobObj - it is a condor obj
            requirementsString = '(HasBeenOverflowRouted is undefined)'
            requirementsString = f'((target.Crab_ReqName == "{jobObj["CRAB_ReqName"]}") && {requirementsString})'
            logging.debug("requirementsString: %s", requirementsString)

            if self.needOverflow(jobObj):
                thisReqString = requirementsString.lower()
                if thisReqString in self.requirementsStringSet:
                    continue
                # OK got a new task to route
                self.requirementsStringSet.add(thisReqString)
                # add nearby T2's to DESIRED_SITES
                targetT1 = jobObj["DESIRED_SITES"]
                self.currSiteList = targetT1
                newSiteList = f"{targetT1},{self.nearbyT2s[targetT1]}"
                logging.debug("newSiteList: %s", newSiteList)
                self.currSiteListDiff = self.nearbyT2s[targetT1]
                self.createJobRouterAd(requirementsString, newSiteList, jobObj["CRAB_ReqName"])
        logging.debug("self.requirementsStringSet: \n%s\n", pformat(self.requirementsStringSet))

    def run(self):
        """
        The function used to run the class.
        """
        # list all idle jobs (avoid HammerCloud) and get intereting ads for them
        results = []
        try:
            constraint = '(JobUniverse==5)  && JobStatus==1 && (CRAB_UserHN != "sciaba") '\
                         '&& ((CMS_ALLOW_OVERFLOW isnt undefined) && (CMS_ALLOW_OVERFLOW == "True"))'
            projection = ["ClusterId",
                          "CRAB_UserHN",
                          "CRAB_ReqName",
                          "DAGManJobId",
                          "JobStatus",
                          "JobUniverse",
                          "QDate",
                          "CMS_ALLOW_OVERFLOW",
                          "DESIRED_SITES"]

            results = self.schedd.query(constraint, projection)

        except Exception as e:  # pylint: disable=broad-except
            warnings.warn( e ,category=ScheddAlarm)

        # Creating a dict of classad.objects from the query result with keys = CRAB_RegName (to be used in the search later).
        jobsInThisSchedd = {}
        for jobAd in results:
            jobsInThisSchedd[jobAd["ClusterId"]] = jobAd
        # logging.debug("jobsInThisSchedd: \n%s\n" pformat( jobsInThisSchedd))

        self.overflow(jobsInThisSchedd)


def main():
    """ do the work """
    logging.info("============  JobAutoTuner-py3 starting ==============")
    enableOverflow = readCondorFlag("JAT_ENABLE_OVERFLOW")
    if enableOverflow:
        logging.info("-------------------- Routes added by Overflow.py  --------------------")
        try:
            overflow = Overflow()
            overflow.run()
        except Exception as e:  # pylint: disable=broad-except
            subScriptAlarm = SubScriptAlarm(f"Overflow failed with exception: {e}")
            warnings.warn(subScriptAlarm, stacklevel=5)
    else:
        logging.info("-------------------- Overflow.py was not enabled  --------------------")
    logging.info("=========================================================================")


if __name__ == "__main__":

    warnings.resetwarnings()
    warnings.simplefilter("always", category=GwmsmonAlarm, lineno=0, append=False)
    warnings.simplefilter("always", category=ConfigAlarm, lineno=0, append=False)
    warnings.simplefilter("always", category=ScheddAlarm, lineno=0, append=False)
    warnings.simplefilter("always", category=SubScriptAlarm, lineno=0, append=False)

    LOG_FILE = '/var/log/crab/JobAutoTuner.log'
    FORMAT_STRING = '%(asctime)s:%(levelname)s:%(module)s,%(lineno)d:%(message)s'
    logFormatter = logging.Formatter(FORMAT_STRING)
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.DEBUG)

    # Setting different loglevels for file logging and console output
    fileHandler = logging.FileHandler(LOG_FILE)
    fileHandler.setFormatter(logFormatter)
    fileHandler.setLevel(logging.INFO)
    rootLogger.addHandler(fileHandler)

    # Setting the output of the StreamHandler to stdout
    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setFormatter(logFormatter)
    consoleHandler.setLevel(logging.CRITICAL)
    rootLogger.addHandler(consoleHandler)

    logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format=FORMAT_STRING)

    with open('/dev/null', 'w', encoding='utf-8') as bitBucket:
        # Temporary redirect stderr
        sys.stderr = bitBucket
        main()
