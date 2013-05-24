# WMCore dependecies here
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Validation import validate_str

# CRABServer dependecies here
from CRABInterface.RESTExtensions import authz_login_valid
from CRABInterface.Regexps import RX_SUBRES_SI


class RESTServerInfo(RESTEntity):
    """REST entity for workflows and relative subresources"""

    def __init__(self, app, api, config, mount, serverdn):
        RESTEntity.__init__(self, app, api, config, mount)
        self.delegatedn = config.delegatedn
        self.serverdn = serverdn

    def validate(self, apiobj, method, api, param, safe):
        """Validating all the input parameter as enforced by the WMCore.REST module"""
        authz_login_valid()
        if method in ['GET']:
            validate_str('subresource', param, safe, RX_SUBRES_SI, optional=False)

    @restcall
    def get(self, subresource):
        """Retrieves the server information, like delegateDN which is the DN of the
           machine who is actually submitting the jobs (The WMAgent).
           :arg str subresource: the specific server information to be accessed;
        """
        return getattr(RESTServerInfo, subresource)(self)

    def delegatedn(self):
        yield {'rest': self.serverdn, 'services': self.delegatedn}
