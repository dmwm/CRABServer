import logging


class TaskAction(object):
    """The ABC of all actions"""

    def __init__(self, config, server = None, resturl = None):
        self.logger = logging.getLogger(type(self).__name__)
        self.config = config
        self.jobtypeMapper = { "Analysis" : "Processing",
                               "PrivateMC" : "Production",
                               "Generic" : "Generic",}
        self.server = server
        self.resturl = resturl
        if server: #when testing this can be none
            self.backendurls = self.server.get(self.resturl.replace('workflowdb', 'info'), data={'subresource':'backendurls'})[0]['result'][0]

    def execute(self):
        raise NotImplementedError

