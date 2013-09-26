# WMCore dependecies here
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Validation import validate_str

# CRABServer dependecies here
from CRABInterface.RESTExtensions import authz_login_valid
from CRABInterface.Regexps import RX_SUBRES_SI
from CRABInterface.Utils import conn_handler


class RESTServerInfo(RESTEntity):
    """REST entity for workflows and relative subresources"""

    def __init__(self, app, api, config, mount, serverdn, centralcfg):
        RESTEntity.__init__(self, app, api, config, mount)
        self.centralcfg = centralcfg
        self.serverdn = serverdn
        #used by the client to get the url where to update the cache (cacheSSL)
        #and by the taskworker Panda plugin to get panda urls

    def validate(self, apiobj, method, api, param, safe):
        """Validating all the input parameter as enforced by the WMCore.REST module"""
        authz_login_valid()
        if method in ['GET']:
            validate_str('subresource', param, safe, RX_SUBRES_SI, optional=False)

    @restcall
    def get(self, subresource):
        """Retrieves the server information, like delegateDN, filecacheurls ...
           :arg str subresource: the specific server information to be accessed;
        """
        return getattr(RESTServerInfo, subresource)(self)

    @conn_handler(services=['centralconfig'])
    def delegatedn(self):
        yield {'services': self.centralcfg.centralconfig['delegate-dn']}

    @conn_handler(services=['centralconfig'])
    def backendurls(self):
        yield self.centralcfg.centralconfig['backend-urls']
