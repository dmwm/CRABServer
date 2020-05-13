import os
import json
import urllib
import logging
from base64 import b64encode
from httplib import HTTPException

from ServerUtilities import truncateError
from RESTInteractions import HTTPRequests

class TaskAction(object):
    """The ABC of all actions"""

    def __init__(self, config, server = '', resturi = '', procnum = -1):
        self.logger = logging.getLogger(str(procnum))
        self.config = config
        self.jobtypeMapper = {'Analysis': 'Processing',
                              'PrivateMC': 'Production',
                              'Generic': 'Generic',
                             }
        self.server = server
        self.procnum = procnum
        # Initialised in DBSDataDiscovery:
        self.dbs = None
        self.dbsInstance = None
        self.tapeLocations = set()

        #self.resturl = resturi # backward compatibility
        ## Trying to give the right naming to the variables.
        ## In the resturl arg we have the REST URI (e.g. '/crabserver/prod/workflowdb').
        ## The first field in the REST URI (e.g. 'crabserver') I will call it the server
        ## API. The second field (e.g. 'prod', 'preprod', 'dev') is the REST database instance
        ## instance. The third field (e.g. 'workflowdb', 'info', 'filemetadata') is the
        ## REST API.
        self.resturi = resturi # everything new should use self.resturi and not self.resturl.
        ## Since 90% of the calls are toward workflowdb the URI default to that REST api
        ## However we are saving the base uri in case the API is different
        self.restURInoAPI = resturi.rsplit('/', 1)[0] ## That's like '/crabserver/prod'
        if server: ## When testing, the server can be None.
            self.backendurls = self.server.get(self.restURInoAPI + '/info', data = {'subresource': 'backendurls'})[0]['result'][0]

    def execute(self):
        raise NotImplementedError


    def uploadWarning(self, warning, userProxy, taskname):
        if not self.server: # When testing, the server can be None
            self.logger.warning(warning)
            return

        truncWarning = truncateError(warning)
        userServer = HTTPRequests(self.server['host'], userProxy, userProxy, retry=2,
                                  logger = self.logger)
        configreq = {'subresource': 'addwarning',
                        'workflow': taskname,
                         'warning': b64encode(truncWarning)}
        try:
            userServer.post(self.restURInoAPI + '/task', data = urllib.urlencode(configreq))
        except HTTPException as hte:
            self.logger.error("Error uploading warning: %s", str(hte))
            self.logger.warning("Cannot add a warning to REST interface. Warning message: %s", warning)


    def deleteWarnings(self, userProxy, taskname):
        userServer = HTTPRequests(self.server['host'], userProxy, userProxy, retry=2,
                                  logger = self.logger)
        configreq = {'subresource': 'deletewarnings',
                        'workflow': taskname}
        try:
            userServer.post(self.restURInoAPI + '/task', data = urllib.urlencode(configreq))
        except HTTPException as hte:
            self.logger.error("Error deleting warnings: %s", str(hte))
            self.logger.warning("Can not delete warnings from REST interface.")


    def getBlacklistedSites(self):
        bannedSites = []
        fileLocation = os.path.join(self.config.TaskWorker.scratchDir, "blacklistedSites.txt")
        if os.path.isfile(fileLocation):
            with open(fileLocation) as fd:
                try:
                    bannedSites = json.load(fd)
                except ValueError as e:
                    self.logger.error("Failed to load json from file %s. Error message: %s", fileLocation, e)
                    return []
        return bannedSites
