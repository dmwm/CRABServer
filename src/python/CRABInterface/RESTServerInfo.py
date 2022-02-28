import logging

# WMCore dependecies here
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Validation import validate_str
from WMCore.REST.Error import ExecutionError

# CRABServer dependecies here
from CRABInterface.RESTExtensions import authz_login_valid
from CRABInterface.Regexps import RX_SUBRES_SI, RX_TASKNAME
from CRABInterface.Utilities import conn_handler
from CRABInterface.__init__ import __version__


class RESTServerInfo(RESTEntity):
    """REST entity for workflows and relative subresources"""

    def __init__(self, app, api, config, mount, centralcfg):
        RESTEntity.__init__(self, app, api, config, mount)
        self.centralcfg = centralcfg
        self.logger = logging.getLogger("CRABLogger:RESTServerInfo")
        #used by the client to get the url where to update the cache (cacheSSL)

    def validate(self, apiobj, method, api, param, safe ):
        """Validating all the input parameter as enforced by the WMCore.REST module"""
        authz_login_valid()
        if method in ['GET']:
            validate_str('subresource', param, safe, RX_SUBRES_SI, optional=True)
            validate_str('workflow', param, safe, RX_TASKNAME, optional=True)

    @restcall
    def get(self, subresource , **kwargs):
        """Retrieves the server information, like delegateDN, filecacheurls ...
           :arg str subresource: the specific server information to be accessed;
        """
        if subresource:
            return getattr(RESTServerInfo, subresource)(self, **kwargs)
        else:
            next(self.api.query(None, None, "select NULL from DUAL")) #Checking database connection
            return [{"crabserver":"Welcome","version":__version__}]

    @conn_handler(services=['centralconfig'])
    def delegatedn(self, **kwargs):
        yield {'services': self.centralcfg.centralconfig['delegate-dn']}

    @conn_handler(services=['centralconfig'])
    def backendurls(self , **kwargs):
        yield self.centralcfg.centralconfig['backend-urls']

    @conn_handler(services=['centralconfig'])
    def version(self , **kwargs):
        yield self.centralcfg.centralconfig['compatible-version']+[__version__]

    @conn_handler(services=['centralconfig'])
    def bannedoutdest(self, **kwargs):
        yield self.centralcfg.centralconfig['banned-out-destinations']

    @conn_handler(services=['centralconfig'])
    def ignlocalityblacklist(self, **kwargs):
        yield self.centralcfg.centralconfig['ign-locality-blacklist']
