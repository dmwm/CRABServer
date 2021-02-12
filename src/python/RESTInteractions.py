"""
Handles client interactions with remote REST interface
"""

import os
import time
import urllib
import logging
from httplib import HTTPException
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
        #500 Internal sever error. For some errors retries it helps
        #502 CMSWEB frontend answers with this when the CMSWEB backends are overloaded
        #503 Usually that's the DatabaseUnavailable error
        return ex.status in [500, 502, 503]
    if isinstance(ex, pycurl.error):
        #28 is 'Operation timed out...'
        #35,is 'Unknown SSL protocol error', see https://github.com/dmwm/CRABServer/issues/5102
        return ex[0] in [28, 35]
    return False


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

    def __init__(self, url='localhost', localcert=None, localkey=None, version=__version__,
                 retry=0, logger=None, verbose=False, userAgent='CRAB?'):
        """
        Initialise an HTTP handler
        """
        dict.__init__(self)
        #set up defaults
        self.setdefault("accept_type", 'text/html')
        self.setdefault("content_type", 'application/x-www-form-urlencoded')
        self.setdefault("host", url)
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
        uri = urllib.quote(uri)
        caCertPath = self.getCACertPath()
        url = 'https://' + self['host'] + uri

        #retries this up to self['retry'] times a range of exit codes
        for i in range(self['retry'] + 1):
            try:
                response, datares = self['conn'].request(url, data, encode=True, headers=headers, verb=verb, doseq=True,
                                                         ckey=self['key'], cert=self['cert'], capath=caCertPath,
                                                         verbose=self['verbose'])
            except Exception as ex:
                #add here other temporary errors we need to retry
                if (not retriableError(ex)) or (i == self['retry']):
                    msg = "Fatal error trying to connect to %s using %s" % (url, data)
                    self.logger.error(msg)
                    raise #really exit and raise exception if this was the last retry or the exit code is not among the list of the one we retry
                sleeptime = 20 * (i + 1)
                msg = "Sleeping %s seconds after HTTP error. Error details:  " % sleeptime
                if hasattr(ex, 'headers'):
                    msg += str(ex.headers)
                else:
                    msg += str(ex)
                self.logger.debug(msg)
                time.sleep(sleeptime) #sleeps 20s the first time, 40s the second time and so on
            else:
                break
        try:
            result = JSONRequests(idict={"pycurl" : True}).decode(datares)
        except Exception as ex:
            msg = "Fatal error reading data from %s using %s" % (url, data)
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
