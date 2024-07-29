"""
a collection of common Task-oriented functions to be used in various places
"""

from http.client import HTTPException
from urllib.parse import urlencode
from ServerUtilities import truncateError


def getTasks(crabserver=None, status=None, logger=None, limit=1000):
    """retrieve from DB a list of tasks with given status"""
    msg = f"Retrieving {status} tasks"
    tasks = []
    configreq = {'limit': limit, 'workername': '%', 'getstatus': status}
    try:
        data = urlencode(configreq)
        tasks = crabserver.get(api='workflowdb', data=data)[0]['result']
    except HTTPException as hte:  # pylint: disable=broad-except
        logger.error("Cannot retrieve task list from crabserver: %s", str(hte))
    if not tasks:
        msg = f"No {status} task retrieved."
        logger.info(msg)
    else:
        msg = f"Retrieved a total of {len(tasks)} {status} tasks"
        logger.info(msg)
    return tasks


def uploadWarning(crabserver=None, taskname=None, msg='', logger=None):
    """ Uploads a warning message to the Task DB so that crab status can show it """
    truncWarning = truncateError(msg)
    configreq = {'subresource': 'addwarning', 'workflow': taskname, 'warning': truncWarning}
    try:
        data = urlencode(configreq)
        crabserver.post(api='task', data=data)
    except HTTPException as hte:
        logger.error("Error uploading warning: %s", str(hte))
        logger.warning("Cannot add a warning to REST interface. Warning message: %s", msg)


def deleteWarnings(crabserver=None, taskname=None, logger=None):
    """ Removes all warnings uploaded so fare for this task """
    configreq = {'subresource': 'deletewarnings', 'workflow': taskname}
    data = urlencode(configreq)
    try:
        crabserver.post(api='task', data=data)
    except HTTPException as hte:
        logger.error("Error deleting warnings: %s", str(hte))
        logger.warning("Can not delete warnings from REST interface.")


def updateTaskStatus(crabserver=None, taskName=None, status=None, logger=None):
    """ change task status in the DB """
    msg = f"Will set to {status} task {taskName}"
    logger.info(msg)
    if status == 'NEW':
        command = 'SUBMIT'
    elif status == 'SUBMITREFUSED':
        command = 'SUBMIT'
    elif status == 'KILLED':
        command = 'KILL'
    else:
        logger.error('updateTaskStatus does not know how to handle status %s. Do nothing', status)
        return
    configreq = {'subresource': 'state', 'workflow': taskName, 'status': status, 'command': command}
    data = urlencode(configreq)
    crabserver.post(api='workflowdb', data=data)
