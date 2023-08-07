"""
Common functions to be reused around TW and Publisher
"""

from http.client import HTTPException
from urllib.parse import urlencode

from ServerUtilities import truncateError


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
