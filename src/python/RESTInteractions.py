"""
    RESTInteractions.py - Wrap WMCore's request API. Needed to support caching 
                     values.
"""
import os
import os.path
from WMCore.Services.Requests import JSONRequests

class HTTPRequests(JSONRequests):
    def __init__(self, url='localhost', localcert=None, localkey=None, version=None):
        if not url.startswith('https://'):
            url = "https://%s" % url
        cachePath = os.getenv('CMS_CRAB_CACHE_DIR', \
                                os.path.expanduser("~/.crab3_cache"))
        extraDict = {'cachepath' : cachePath,
                'serice_name' : 'https://github.com/dmwm/CRABServer'}
        JSONRequests.__init__(self, url, idict = extraDict)
        if not version:
            version = "CRABServer/v001"
        self.additionalHeaders = {"User-agent": 
                                        version}
        if localcert:
            self['cert'] = localcert
        if localkey:
            self['key'] = localkey
    
    def makeRequest(self, uri = None, data = {}, verb = 'GET', incoming_headers={},
                            encoder = True, decoder = True, contentType = None):

        data, responseStatus, responseReason, responseFromCache = \
                JSONRequests.makeRequest(self, uri, data, 
                                         verb, incoming_headers,
                                         encoder, decoder,
                                         contentType)

        return data, responseStatus, responseReason

    @staticmethod
    def getCACertPath():
        """ Get the CA certificate path. It looks for it in the X509_CERT_DIR variable if present
            or return /etc/grid-security/certificates/ instead (if it exists)
            If a CA certificate path cannot be found throws a EnvironmentException exception
        """
        caDefault = '/etc/grid-security/certificates/'
        if os.environ.has_key("X509_CERT_DIR"):
            return os.environ["X509_CERT_DIR"]
        elif os.path.isdir(caDefault):
            return caDefault
        else:
            raise EnvironmentException("The X509_CERT_DIR variable is not set and the %s directory cannot be found.\n" % caDefault +
                                        "Cannot find the CA certificate path to ahuthenticate the server.")
