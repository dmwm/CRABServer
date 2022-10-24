import os
import json
import logging
from http.client import HTTPException
import sys
if sys.version_info >= (3, 0):
    from urllib.parse import urlencode  # pylint: disable=no-name-in-module
if sys.version_info < (3, 0):
    from urllib import urlencode

from ServerUtilities import truncateError

class TaskAction(object):
    """The ABC of all actions"""

    def __init__(self, config, crabserver='', procnum=-1):
        self.logger = logging.getLogger(str(procnum))
        self.config = config
        self.jobtypeMapper = {'Analysis': 'Processing',
                              'PrivateMC': 'Production',
                              'Generic': 'Generic',
                             }
        self.crabserver = crabserver
        self.procnum = procnum
        # Initialised in DBSDataDiscovery:
        self.dbs = None
        self.dbsInstance = None
        self.tapeLocations = set()

        if crabserver:  # When testing, the server can be None.
            self.backendurls = self.crabserver.get(api='info', data={'subresource': 'backendurls'})[0]['result'][0]

    def execute(self, *args, **kwargs):
        raise NotImplementedError


    def uploadWarning(self, warning, userProxy, taskname):
        """
        Uploads a warning message to the Task DB so that crab status can show it
        :param warning: string: message text
        :param userProxy: credential to use for the http POST call
                           Stefano does not know why user proxy is used here instead of TW proxy,
                           maybe some early version of the REST checked that POST was done by task owner,
                           maybe some early developer feared that it would fail, but there are places where
                           TW internal credential is used to change status of the task. So this could be
                           investigate, cleaned up and possibly simplified. But... since it works..
        :param taskname:
        :return:
        """
        if not self.crabserver: # When testing, the server can be None
            self.logger.warning(warning)
            return

        truncWarning = truncateError(warning)
        configreq = {'subresource': 'addwarning',
                     'workflow': taskname,
                     'warning': truncWarning}
        try:
            self.crabserver.post(api='task', data=urlencode(configreq))
        except HTTPException as hte:
            self.logger.error("Error uploading warning: %s", str(hte))
            self.logger.warning("Cannot add a warning to REST interface. Warning message: %s", warning)


    def deleteWarnings(self, userProxy, taskname):
        configreq = {'subresource': 'deletewarnings', 'workflow': taskname}
        try:
            self.crabserver.post(api='task', data=urlencode(configreq))
        except HTTPException as hte:
            self.logger.error("Error deleting warnings: %s", str(hte))
            self.logger.warning("Can not delete warnings from REST interface.")

    def loadJSONFromFileInScratchDir(self, path):
        fileLocation = os.path.join(self.config.TaskWorker.scratchDir, path)
        if os.path.isfile(fileLocation):
            with open(fileLocation, 'r', encoding='utf-8') as fd:
                try:
                    sites = json.load(fd)
                except ValueError as e:
                    self.logger.error("Failed to load json from file %s. Error message: %s", fileLocation, e)
                    return []
        return sites
