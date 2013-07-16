# WMCore dependecies here
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Validation import validate_str

# CRABServer dependecies here
from CRABInterface.RESTExtensions import authz_login_valid
from CRABInterface.Regexps import RX_SUBRES_SI


class RESTServerInfo(RESTEntity):
    """REST entity for workflows and relative subresources"""

    def __init__(self, app, api, config, mount, serverdn, delegatedn, backendurls):
        RESTEntity.__init__(self, app, api, config, mount)
        self.delegatedn = delegatedn
        self.serverdn = serverdn
        #used by the client to get the url where to update the cache (cacheSSL)
        #and by the taskworker Panda plugin to get panda urls
        self.backendurls = backendurls

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

    def delegatedn(self):
        yield {'rest': self.serverdn, 'services': self.delegatedn}

    def backendurls(self):
        yield self.backendurls
