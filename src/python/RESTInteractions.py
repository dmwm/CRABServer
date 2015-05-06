"""
Handles client interactions with remote REST interface
"""

import os
import time
import urllib
import logging
from urlparse import urlunparse
from httplib import HTTPException

from WMCore.Services.Requests import JSONRequests
from WMCore.Services.pycurl_manager import RequestHandler

__version__= '0.0.0'
EnvironmentException = Exception


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

    def __init__(self, url='localhost', localcert=None, localkey=None, version=None, retry=0, logger=None):
        """
        Initialise an HTTP handler
        """
        #set up defaults
        self.setdefault("accept_type", 'text/html')
        self.setdefault("content_type", 'application/x-www-form-urlencoded')
        self.setdefault("host", url)
        #self.setdefault("proxyfilename", proxyfilename)
        self.setdefault("cert", localcert)
        self.setdefault("key", localkey)
        # get the URL opener
        self.setdefault("conn", self.getUrlOpener())
        if not version:
            version = __version__
        self.setdefault("version", version)
        self.setdefault("retry", retry)
        self.logger = logger if logger else logging.getLogger()

    def getUrlOpener(self):
        """
        method getting an HTTPConnection, it is used by the constructor such
        that a sub class can override it to have different type of connection
        i.e. - if it needs authentication, or some fancy handler
        """
        return RequestHandler(config={'timeout': 300, 'connecttimeout' : 300})

    def get(self, uri = None, data = {}):
        """
        GET some data
        """
        return self.makeRequest(uri = uri, data = data, verb = 'GET')

    def post(self, uri = None, data = {}):
        """
        POST some data
        """
        return self.makeRequest(uri = uri, data = data, verb = 'POST')

    def put(self, uri = None, data = {}):
        """
        PUT some data
        """
        return self.makeRequest(uri = uri, data = data, verb = 'PUT')

    def delete(self, uri = None, data = {}):
        """
        DELETE some data
        """
        return self.makeRequest(uri = uri, data = data, verb = 'DELETE')

    def makeRequest(self, uri = None, data = {}, verb = 'GET',
                     encoder = True, decoder = True, contentType = None):
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
        headers = {
                   "User-agent": "CRABClient/%s" % self['version'],
                   "Accept": "*/*",
                  }

        #Quoting the uri since it can contain the request name, and therefore spaces (see #2557)
        uri = urllib.quote(uri)
        caCertPath = self.getCACertPath()
        url = 'https://' + self['host'] + uri

        #retries this up to self['retry'] times if the exit code is 503 (the only code at the moemnt)
        for i in xrange(self['retry'] + 1):
            try:
                response, datares = self['conn'].request(url, data, headers, verb=verb, doseq = True, ckey=self['key'], cert=self['cert'], \
                                capath=caCertPath)#, verbose=True)# for debug
            except HTTPException as ex:
                #add here other temporary errors we need to retry
                if ex.status not in [500, 502, 503] or i == self['retry']:
                    raise #really exit and raise exception it this was the last retry or the exit code is not among the list of the one we retry
                sleeptime = 20 * (i + 1)
                self.logger.debug("Sleeping %s seconds after HTTP error. Error details %s:", sleeptime, ex.headers)
                time.sleep(sleeptime) #sleeps 20s the first time, 40s the second time and so on
            else:
                break

        return self.decodeJson(datares), response.status, response.reason

    def decodeJson(self, result):
        """
        decodeJson

        decode the response result reveiced from the server
        """
        encoder = JSONRequests()
        return encoder.decode(result)


    def buildUrl(self, uri):
        """
        Prepares the remote URL
        """
        scheme = 'https'
        netloc = '%s:%s' % (self['conn'].host, self['conn'].port)
        return urlunparse([scheme, netloc, uri, '', '', ''])

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
