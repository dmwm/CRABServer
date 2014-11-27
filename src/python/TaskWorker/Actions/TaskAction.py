import logging


class TaskAction(object):
    """The ABC of all actions"""

    def __init__(self, config, server = None, resturl = None):
        self.logger = logging.getLogger(type(self).__name__)
        self.config = config
        self.jobtypeMapper = {'Analysis'  : 'Processing',
                              'PrivateMC' : 'Production',
                              'Generic'   : 'Generic',
                             }
        self.server = server
        self.resturl = resturl
        ## Trying to give the right naming to the variables.
        ## In the resturl arg we have the REST URI (e.g. '/crabserver/prod/workflowdb').
        ## The first field in the REST URI (e.g. 'crabserver') I will call it the server
        ## API. The second field (e.g. 'prod', 'preprod', 'dev') is the REST database
        ## instance. The third field (e.g. 'workflowdb', 'info', 'filemetadata') is the
        ## REST API.
        self.rest_uri = resturl
        self.server_api, self.rest_db_instance, self.rest_api = self.rest_uri.split('/')[1:]
        self.rest_uri_no_api = '/' + self.server_api + '/' + self.rest_db_instance
        if server: ## When testing, the server can be None.
            rest_api = 'info'
            rest_uri = self.rest_uri_no_api + '/' + rest_api
            self.backendurls = self.server.get(rest_uri, data = {'subresource': 'backendurls'})[0]['result'][0]

    def execute(self):
        raise NotImplementedError

