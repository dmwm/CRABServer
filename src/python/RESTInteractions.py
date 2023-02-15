"""
Handles client interactions with remote REST interface
"""

import os
import time
import random

from urllib.parse import quote as urllibQuote

import logging
from http.client import HTTPException
import pycurl

from WMCore.Services.Requests import JSONRequests
from WMCore.Services.pycurl_manager import RequestHandler

try:
    from TaskWorker import __version__
except:  # pylint: disable=bare-except
    try:
        from CRABClient import __version__
    except:  # pylint: disable=bare-except
        __version__ = '0.0.0'

EnvironmentException = Exception


def retriableError(ex):
    """ Return True if the error can be retried
    """
    if isinstance(ex, HTTPException):
        #403 Authentication failure. When CMSWEB FrontEnd is restarting
        #429 Too Many Requests. When client hits the throttling limit
        #500 Internal sever error. For some errors retries it helps
        #502 CMSWEB frontend answers with this when the CMSWEB backends are overloaded
        #503 Usually that's the DatabaseUnavailable error
        return ex.status in [403, 429, 500, 502, 503]
    if isinstance(ex, pycurl.error):
        #28 is 'Operation timed out...'
        #35,is 'Unknown SSL protocol error', see https://github.com/dmwm/CRABServer/issues/5102
        return ex.args[0] in [28, 35]
    return False

def terminalError(ex):
    """
    Return True if HTTP server reported a clear "no way" which does
    not make sense to retry
    """
    #406 NotAcceptable (server understand what client wants, but refuses)
    #405 Unsupported Method
    #404 NoSuchInstance (typically when trying a DB instance not supported by a given host)
    #401 Unauthorized (usually we do not get this but 403, but meaning is the same. Future proofing)
    #400 BadRequest (more generic refusal of this request by HTTP server)
    terminal = False
    if isinstance(ex, HTTPException) and \
            ex.status in [400, 401, 404, 405, 406]:
        terminal = True
    return terminal


class HTTPRequests(dict):
    """
    This code is a simplified version of WMCore.Services.Requests - we don't
    need all the bells and whistles here since data is always sent via json we
    also move the encoding of data out of the makeRequest.

    HTTPRequests does no logging or exception handling, these are managed by the
    Client class that instantiates it.

    NOTE: This class should be replaced by the WMCore.Services.JSONRequests if WMCore
    is used more in the client.
    """

    def __init__(self, hostname='localhost', localcert=None, localkey=None, version=__version__,
                 retry=0, logger=None, verbose=False, userAgent='CRAB?'):
        """
        Initialise an HTTP handler
        """
        dict.__init__(self)
        #set up defaults
        self.setdefault("accept_type", 'text/html')
        self.setdefault("content_type", 'application/x-www-form-urlencoded')
        self.setdefault("host", hostname)
        # setup port 8443 for cmsweb services (leave them alone things like personal private VM's)
        if self['host'].startswith("https://cmsweb") or self['host'].startswith("cmsweb"):
            if self['host'].endswith(':8443'):
                # good to go
                pass
            elif ':' in self['host']:
                # if there is a port number already, trust it
                pass
            else:
                # add port 8443
                self['host'] = self['host'].replace(".cern.ch", ".cern.ch:8443", 1)
        self.setdefault("cert", localcert)
        self.setdefault("key", localkey)
        # get the URL opener
        self.setdefault("conn", self.getUrlOpener())
        self.setdefault("version", version)
        self.setdefault("retry", retry)
        self.setdefault("verbose", verbose)
        self.setdefault("userAgent", userAgent)
        self.logger = logger if logger else logging.getLogger()

    def getUrlOpener(self):
        """
        method getting an HTTPConnection, it is used by the constructor such
        that a sub class can override it to have different type of connection
        i.e. - if it needs authentication, or some fancy handler
        """
        return RequestHandler(config={'timeout': 300, 'connecttimeout' : 300})

    def get(self, uri=None, data=None):
        """
        GET some data
        """
        return self.makeRequest(uri=uri, data=data, verb='GET')

    def post(self, uri=None, data=None):
        """
        POST some data
        """
        return self.makeRequest(uri=uri, data=data, verb='POST')

    def put(self, uri=None, data=None):
        """
        PUT some data
        """
        return self.makeRequest(uri=uri, data=data, verb='PUT')

    def delete(self, uri=None, data=None):
        """
        DELETE some data
        """
        return self.makeRequest(uri=uri, data=data, verb='DELETE')

    def makeRequest(self, uri=None, data=None, verb='GET'):
        """
        Make a request to the remote database. for a give URI. The type of
        request will determine the action take by the server (be careful with
        DELETE!). Data should be a dictionary of {dataname: datavalue}.

        Returns a tuple of the data from the server, decoded using the
        appropriate method the response status and the response reason, to be
        used in error handling.

        You can override the method to encode/decode your data by passing in an
        encoding/decoding function to this method. Your encoded data must end up
        as a string.
        """
        data = data or {}
        headers = {
            "User-agent": "%s/%s" % (self['userAgent'], self['version']),
            "Accept": "*/*",
        }

        #Quoting the uri since it can contain the request name, and therefore spaces (see #2557)
        uri = urllibQuote(uri)
        caCertPath = self.getCACertPath()
        url = 'https://' + self['host'] + uri

        # retries this up at least 3 times, or up to self['retry'] times for range of exit codes
        # retries are counted AFTER 1st try, so call is made up to nRetries+1 times !
        nRetries = max(2, self['retry'])
        for i in range(nRetries +1):
            try:
                response, datares = self['conn'].request(url, data, encode=True, headers=headers, verb=verb, doseq=True,
                                                         ckey=self['key'], cert=self['cert'], capath=caCertPath,
                                                         verbose=self['verbose'])
            except Exception as ex:
                if terminalError(ex):
                    # HTTP went OK, but operation is definitely not possible
                    msg = "Rejected request when connecting to %s using %s. Error details: " % (url, data)
                    msg = msg + str(ex.headers) if hasattr(ex, 'headers') else msg + str(ex)
                    self.logger.error(msg)
                    raise
                if (i < 2) or (retriableError(ex) and (i < self['retry'])):
                    sleeptime = 20 * (i + 1) + random.randint(-10, 10)
                    msg = "Sleeping %s seconds after HTTP error. Error details:  " % sleeptime
                    if hasattr(ex, 'headers'):
                        msg += str(ex.headers)
                    else:
                        msg += str(ex)
                    self.logger.debug(msg)
                    time.sleep(sleeptime)
                else:
                    # this was the last retry
                    msg = "Fatal error trying to connect to %s using %s. Error details: " % (url, data)
                    msg = msg+str(ex.headers) if hasattr(ex, 'headers') else msg+str(ex)
                    self.logger.error(msg)
                    raise

            else:
                break
        try:
            idict = {
                "pycurl": True,
                # WMCore's Requests object (parent of JSONRequests) requires the following two lines as well
                "cert": self['cert'],
                "key": self['key'],
            }
            result = JSONRequests(idict=idict).decode(datares)
        except Exception as ex:
            msg = "Fatal error reading data from %s using %s:\n%s" % (url, data, ex)
            self.logger.error(msg)
            raise #really exit and raise exception
        return result, response.status, response.reason

    @staticmethod
    def getCACertPath():
        """ Get the CA certificate path. It looks for it in the X509_CERT_DIR variable if present
            or return /etc/grid-security/certificates/ instead (if it exists)
            If a CA certificate path cannot be found throws a EnvironmentException exception
        """
        caDefault = '/etc/grid-security/certificates/'
        if "X509_CERT_DIR" in os.environ:
            return os.environ["X509_CERT_DIR"]
        if os.path.isdir(caDefault):
            return caDefault
        raise EnvironmentException("The X509_CERT_DIR variable is not set and the %s directory cannot be found.\n" % caDefault +
                                   "Cannot find the CA certificate path to authenticate the server.")

class CRABRest:
    """
    A convenience class to communicate with CRABServer REST
    Encapsulates an HTTPRequest object (which can be used also with other HTTP servers)
    together with the CRAB DB instance and allows to specify simply the CRAB Server API in
    the various HTTP methods.

    Add two methods to set and get the DB instance
    """
    def __init__(self, hostname='localhost', localcert=None, localkey=None, version=__version__,
                 retry=0, logger=None, verbose=False, userAgent='CRAB?'):
        self.server = HTTPRequests(hostname, localcert, localkey, version,
                                   retry, logger, verbose, userAgent)
        instance = 'prod'
        self.uriNoApi = '/crabserver/' + instance + '/'

    def setDbInstance(self, dbInstance='prod'):
        self.uriNoApi = '/crabserver/' + dbInstance + '/'

    def getDbInstance(self):
        return self.uriNoApi.rstrip('/').split('/')[-1]

    def get(self, api=None, data=None):
        uri = self.uriNoApi + api
        return self.server.get(uri, data)

    def post(self, api=None, data=None):
        uri = self.uriNoApi + api
        return self.server.post(uri, data)

    def put(self, api=None, data=None):
        uri = self.uriNoApi + api
        return self.server.put(uri, data)

    def delete(self, api=None, data=None):
        uri = self.uriNoApi + api
        return self.server.delete(uri, data)
