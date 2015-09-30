# WMCore dependecies here
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Validation import validate_str
from WMCore.REST.Error import ExecutionError
# CRABServer dependecies here
from CRABInterface.RESTExtensions import authz_login_valid
from CRABInterface.Regexps import RX_SUBRES_SI, RX_WORKFLOW
from CRABInterface.Utils import conn_handler
from CRABInterface import __version__
import logging
import HTCondorLocator

class RESTServerInfo(RESTEntity):
    """REST entity for workflows and relative subresources"""

    def __init__(self, app, api, config, mount, serverdn, centralcfg):
        RESTEntity.__init__(self, app, api, config, mount)
        self.centralcfg = centralcfg
        self.serverdn = serverdn
        self.logger = logging.getLogger("CRABLogger:RESTServerInfo")
        #used by the client to get the url where to update the cache (cacheSSL)
        #and by the taskworker Panda plugin to get panda urls

    @classmethod
    def validate(cls, apiobj, method, api, param, safe):
        """Validating all the input parameter as enforced by the WMCore.REST module"""
        del apiobj # unused
        del api # unused
        authz_login_valid()
        if method in ['GET']:
            validate_str('subresource', param, safe, RX_SUBRES_SI, optional=True)
            validate_str('workflow', param, safe, RX_WORKFLOW, optional=True)

    @restcall
    def get(self, subresource, **kwargs):
        """Retrieves the server information, like delegateDN, filecacheurls ...
           :arg str subresource: the specific server information to be accessed;
        """
        if subresource:
            return getattr(RESTServerInfo, subresource)(self, **kwargs)
        else:
            next(self.api.query(None, None, "select NULL from DUAL")) #Checking database connection
            return [{"crabserver":"Welcome", "version":__version__}]

    @conn_handler(services=['centralconfig'])
    def delegatedn(self, **kwargs):
        """Returns delegated DN`s from RestConfiguration"""
        del kwargs # unused
        yield {'services': self.centralcfg.centralconfig['delegate-dn']}

    @conn_handler(services=['centralconfig'])
    def backendurls(self, **kwargs):
        """Returns backendURLS from RestConfiguration"""
        del kwargs # unused
        yield self.centralcfg.centralconfig['backend-urls']

    @conn_handler(services=['centralconfig'])
    def version(self, **kwargs):
        """Returns compatible client versions from RestConfiguration"""
        del kwargs # unused
        yield self.centralcfg.centralconfig['compatible-version']+[__version__]

    @conn_handler(services=['centralconfig'])
    def scheddaddress(self, **kwargs):
        """Returns schedd address of given workflow"""
        backendurl = self.centralcfg.centralconfig['backend-urls']
        workflow = kwargs['workflow']
        try:
            loc = HTCondorLocator.HTCondorLocator(backendurl)
            _, _ = loc.getScheddObj(workflow)
        except Exception as ex:
            self.logger.exception(ex)
            raise ExecutionError("Unable to get schedd address for task %s. Error: %s" % (workflow, ex))
        yield loc.scheddAd['Machine']

    @conn_handler(services=['centralconfig'])
    def bannedoutdest(self, **kwargs):
        """Returns banned destination sites from RestConfiguration"""
        del kwargs # unused
        yield self.centralcfg.centralconfig['banned-out-destinations']
