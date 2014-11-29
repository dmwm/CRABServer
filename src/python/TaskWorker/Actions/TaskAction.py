import logging


class TaskAction(object):
    """The ABC of all actions"""

    def __init__(self, config, server = None, resturi = None):
        self.logger = logging.getLogger(type(self).__name__)
        self.config = config
        self.jobtypeMapper = {'Analysis'  : 'Processing',
                              'PrivateMC' : 'Production',
                              'Generic'   : 'Generic',
                             }
        self.server = server
#        self.resturl = resturi #backward compatibility
        ## Trying to give the right naming to the variables.
        ## In the resturl arg we have the REST URI (e.g. '/crabserver/prod/workflowdb').
        ## The first field in the REST URI (e.g. 'crabserver') I will call it the server
        ## API. The second field (e.g. 'prod', 'preprod', 'dev') is the REST database
        ## instance. The third field (e.g. 'workflowdb', 'info', 'filemetadata') is the
        ## REST API.
        self.resturi = resturi # everything new should use self.resturi and not self.resturl.
        ## Since 90% of the calls are toward workflowdb the URI default to that REST api
        ## However we are saving the base uri in case the API is different
        self.restURInoAPI = resturi.rsplit('/',1)[0] ## That's like '/crabserver/prod'
        if server: ## When testing, the server can be None.
            self.backendurls = self.server.get(self.restURInoAPI + '/info', data = {'subresource': 'backendurls'})[0]['result'][0]

    def execute(self):
        raise NotImplementedError

