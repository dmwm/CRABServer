"""
Common functions to be reused around TW and Publisher
"""

import logging
import functools
from http.client import HTTPException
from urllib.parse import urlencode

from ServerUtilities import truncateError, tempSetLogLevel, SERVICE_INSTANCES
from RESTInteractions import CRABRest
from WMCore.Services.CRIC.CRIC import CRIC
from TaskWorker.WorkerExceptions import ConfigException

def getCrabserver(restConfig=None, agentName='crabtest', logger=None):
    """
    given a configuration object which contains instance, cert and key
    builds a crabserver object. It allows to set agent name so that
    requests by different clients can be separately monitored
    """

    try:
        instance = restConfig.instance
    except AttributeError as exc:
        msg = "No instance provided: need to specify restConfig.instance in the configuration"
        raise ConfigException(msg) from exc

    if instance in SERVICE_INSTANCES:
        logger.info('Will connect to CRAB service: %s', instance)
        restHost = SERVICE_INSTANCES[instance]['restHost']
        dbInstance = SERVICE_INSTANCES[instance]['dbInstance']
    else:
        msg = f"Invalid instance value '{instance}'"
        raise ConfigException(msg)
    if instance == 'other':
        logger.info('Will use restHost and dbInstance from config file')
        try:
            restHost = restConfig.restHost
            dbInstance = restConfig.dbInstance
        except AttributeError as exc:
            msg = "Need to specify restConfig.restHost and dbInstance in the configuration"
            raise ConfigException(msg) from exc

    # Let's increase the server's retries for recoverable errors in the MasterWorker
    # 20 means we'll keep retrying for about 1 hour
    # we wait at 20*NUMRETRY seconds after each try, so retry at: 20s, 60s, 120s ... 20*(n*(n+1))/2
    crabserver = CRABRest(restHost, restConfig.cert, restConfig.key, retry=20,
                               logger=logger, userAgent=agentName)
    crabserver.setDbInstance(dbInstance)

    logger.info('Will connect to CRAB REST via: https://%s/crabserver/%s', restHost, dbInstance)

    return crabserver


def uploadWarning(warning=None, taskname=None, crabserver=None, logger=None):
    """
    Uploads a warning message to the Task DB so that crab status can show it
    :param warning: string: message text
    :param taskname: string: name of the task
    :param crabserver: an instance of CRABRest class
    :param logger: logger
    :return:
    """

    if not crabserver:  # When testing, the server can be None
        logger.warning(warning)
        return

    truncWarning = truncateError(warning)
    configreq = {'subresource': 'addwarning',
                 'workflow': taskname,
                 'warning': truncWarning}
    try:
        crabserver.post(api='task', data=urlencode(configreq))
    except HTTPException as hte:
        logger.error("Error uploading warning: %s", str(hte))
        logger.warning("Cannot add a warning to REST interface. Warning message: %s", warning)


def deleteWarnings(taskname=None, crabserver=None, logger=None):
    """
    deletes all warning messages uploaed for a task
    """
    configreq = {'subresource': 'deletewarnings', 'workflow': taskname}
    try:
        crabserver.post(api='task', data=urlencode(configreq))
    except HTTPException as hte:
        logger.error("Error deleting warnings: %s", str(hte))
        logger.warning("Can not delete warnings from REST interface.")


def safeGet(obj, key, default=None):
    """
    Try dictionary-style access first, otherwise try attribute access.
    """
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def suppressExternalServiceLogging(func):
    """
    Suppresses logging for a function of external service.
    
    Note: Assumes `self.logger` is defined.

    Example:
        @suppressExternalServiceLogging
        def some_external_call(self, ...):
            # External call with suppressed logging
            ...
    """

    @functools.wraps(func)
    def _wrapper(self, *args, **kwargs):
        with tempSetLogLevel(
            logger=safeGet(self, "logger", default=logging.getLogger()),
            level=logging.ERROR,
        ):
            return func(self, *args, **kwargs)

    return _wrapper


class CRICService(CRIC):
    """
    WMCore's CRIC Service with logging suppressed.
    """

    def __init__(self, *args, **kwargs):
        with tempSetLogLevel(logger=kwargs["logger"], level=logging.ERROR):
            super().__init__(*args, **kwargs)

    @suppressExternalServiceLogging
    def _getResult(self, *args, **kwargs):
        """ override super class method to suppress logging """
        return super()._getResult(*args, **kwargs)

    @suppressExternalServiceLogging
    def PNNstoPSNs(self, *args, **kwargs):
        """ maps PhexedNodeNames (i.e. RSE's) to ProcessingSiteNames (i.e. sites) """
        return super().PNNstoPSNs(*args, **kwargs)
