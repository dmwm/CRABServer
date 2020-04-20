import logging
import os
from collections import namedtuple
from time import mktime, gmtime
import re
from hashlib import sha1
import cherrypy
import pycurl
import StringIO
import json
import threading

from WMCore.WMFactory import WMFactory
from WMCore.REST.Error import ExecutionError, InvalidParameter
from WMCore.Services.CRIC.CRIC import CRIC
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Credential.SimpleMyProxy import SimpleMyProxy, MyProxyException
from WMCore.Credential.Proxy import Proxy
from WMCore.Services.pycurl_manager import ResponseHeader

from CRABInterface.Regexps import RX_CERT
"""
The module contains some utility functions used by the various modules of the CRAB REST interface
"""

CMSSitesCache = namedtuple("CMSSitesCache", ["cachetime", "sites"])
ConfigCache = namedtuple("ConfigCache", ["cachetime", "centralconfig"])

#These parameters are set in the globalinit (called in RESTBaseAPI)
serverCert = None
serverKey = None
serverDN = None
credServerPath = None

def getDBinstance(config, namespace, name):
    if config.backend.lower() == 'mysql':
        backend = 'MySQL'
    elif config.backend.lower() == 'oracle':
        backend = 'Oracle'

    #factory = WMFactory(name = 'TaskQuery', namespace = 'Databases.TaskDB.%s.Task' % backend)
    factory = WMFactory(name = name, namespace = 'Databases.%s.%s.%s' % (namespace, backend, name))

    return factory.loadObject( name )

def globalinit(serverkey, servercert, serverdn, credpath):
    global serverCert, serverKey, serverDN, credServerPath
    serverCert, serverKey, serverDN, credServerPath = servercert, serverkey, serverdn, credpath

def execute_command(command, logger, timeout):
    """
    _execute_command_
    Funtion to manage commands.
    """

    stdout, stderr, rc = None, None, 99999
    proc = subprocess.Popen(
            command, shell=True, cwd=os.environ['PWD'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
    )

    t_beginning = time.time()
    seconds_passed = 0
    while True:
        if proc.poll() is not None:
            break
        seconds_passed = time.time() - t_beginning
        if timeout and seconds_passed > timeout:
            proc.terminate()
            logger.error('Timeout in %s execution.' % command )
            return stdout, rc

        time.sleep(0.1)

    stdout, stderr = proc.communicate()
    rc = proc.returncode

    logger.debug('Executing : \n command : %s\n output : %s\n error: %s\n retcode : %s' % (command, stdout, stderr, rc))

    return stdout, rc


#This is used in case git is down for more than 30 minutes
centralCfgFallback = None
def getCentralConfig(extconfigurl, mode):
    """Utility to retrieve the central configuration to be used for dynamic variables
    arg str extconfigurl: the url pointing to the exteranl configuration parameter
    arg str mode: also known as the variant of the rest (prod, preprod, dev, private)
    return: the dictionary containing the external configuration for the selected mode."""

    global centralCfgFallback

    def retrieveConfig(externalLink):

        hbuf = StringIO.StringIO()
        bbuf = StringIO.StringIO()

        curl = pycurl.Curl()
        curl.setopt(pycurl.URL, externalLink)
        curl.setopt(pycurl.WRITEFUNCTION, bbuf.write)
        curl.setopt(pycurl.HEADERFUNCTION, hbuf.write)
        curl.setopt(pycurl.FOLLOWLOCATION, 1)
        curl.perform()
        curl.close()

        header = ResponseHeader(hbuf.getvalue())
        if (header.status < 200 or header.status >= 300):
            msg = "Reading %s returned %s." % (externalLink, header.status)
            if centralCfgFallback:
                msg += "\nUsing cached values for external configuration."
                cherrypy.log(msg)
                return centralCfgFallback
            else:
                cherrypy.log(msg)
                raise ExecutionError("Internal issue when retrieving external configuration from %s" % externalLink)        
        jsonConfig = bbuf.getvalue() 
        
        return jsonConfig

    extConfCommon = json.loads(retrieveConfig(extconfigurl))

    # below 'if' condition is only added for the transition period from the old config file to the new one. It should be removed after some time.
    if 'modes' in extConfCommon:
        extConfSchedds = json.loads(retrieveConfig(extConfCommon['htcondorScheddsLink']))

        # The code below constructs dict from below provided JSON structure
        # {   u'htcondorPool': '', u'compatible-version': [''], u'htcondorScheddsLink': '',
        #     u'modes': [{
        #         u'mode': '', u'backend-urls': {
        #             u'asoConfig': [{ u'couchURL': '', u'couchDBName': ''}],
        #             u'htcondorSchedds': [''], u'cacheSSL': '', u'baseURL': ''}}],
        #     u'banned-out-destinations': [], u'delegate-dn': ['']}
        # to match expected dict structure which is:
        # {   u'compatible-version': [''], u'htcondorScheddsLink': '',
        #     'backend-urls': {
        #         u'asoConfig': [{u'couchURL': '', u'couchDBName': ''}],
        #         u'htcondorSchedds': {u'crab3@vocmsXXXX.cern.ch': {u'proxiedurl': '', u'weightfactor': 1}},
        #         u'cacheSSL': '', u'baseURL': '', 'htcondorPool': ''},
        #     u'banned-out-destinations': [], u'delegate-dn': ['']}
        extConfCommon['backend-urls'] = next((item['backend-urls'] for item in extConfCommon['modes'] if item['mode'] == mode), None)
        extConfCommon['backend-urls']['htcondorPool'] = extConfCommon.pop('htcondorPool')
        del extConfCommon['modes']

        # if htcondorSchedds": [] is not empty, it gets populated with the specified list of schedds,
        # otherwise it takes default list of schedds
        if extConfCommon['backend-urls']['htcondorSchedds']:
            extConfCommon['backend-urls']['htcondorSchedds'] = {k: v for k, v in extConfSchedds.items() if
                                                                k in extConfCommon['backend-urls']['htcondorSchedds']}
        else:
            extConfCommon["backend-urls"]["htcondorSchedds"] = extConfSchedds
        centralCfgFallback = extConfCommon
    else:
        centralCfgFallback = extConfCommon[mode]
        
    return centralCfgFallback


def conn_handler(services):
    """
    Decorator to be used among REST resources to optimize connections to other services
    as CouchDB and CRIC, PhEDEx, WMStats monitoring

    arg str list services: list of string telling which service connections
                           should be started; currently availables are
                           'monitor' and 'asomonitor'.
    """
    def wrap(func):
        def wrapped_func(*args, **kwargs):
            if 'cric' in services and (not args[0].allCMSNames.sites or (args[0].allCMSNames.cachetime+1800 < mktime(gmtime()))):
                args[0].allCMSNames = CMSSitesCache(sites=CRIC().getAllPSNs(), cachetime=mktime(gmtime()))
                args[0].allPNNNames = CMSSitesCache(sites=CRIC().getAllPhEDExNodeNames(), cachetime=mktime(gmtime()))
            if 'phedex' in services and not args[0].phedex:
                phdict = args[0].phedexargs
                phdict.update({'cert': serverCert, 'key': serverKey})
                args[0].phedex = PhEDEx(responseType='xml', dict=phdict)
            if 'centralconfig' in services and (not args[0].centralcfg.centralconfig or (args[0].centralcfg.cachetime+1800 < mktime(gmtime()))):
                args[0].centralcfg = ConfigCache(centralconfig=getCentralConfig(extconfigurl=args[0].config.extconfigurl, mode=args[0].config.mode), cachetime=mktime(gmtime()))
            if 'servercert' in services:
                args[0].serverCert = serverCert
                args[0].serverKey = serverKey
            return func(*args, **kwargs)
        return wrapped_func
    return wrap

def retrieveUserCert(func):
    def wrapped_func(*args, **kwargs):
        logger = logging.getLogger("CRABLogger.Utils")
        myproxyserver = "myproxy.cern.ch"
        userdn = kwargs['userdn']
        defaultDelegation = {'logger': logger,
                             'proxyValidity': '192:00',
                             'min_time_left': 36000,
                             'server_key': serverKey,
                             'server_cert': serverCert,}
        timeleftthreshold = 60 * 60 * 24
        mypclient = SimpleMyProxy(defaultDelegation)
        userproxy = None
        userhash  = sha1(kwargs['userdn']).hexdigest()
        if serverDN:
            try:
                userproxy = mypclient.logonRenewMyProxy(username=userhash, myproxyserver=myproxyserver, myproxyport=7512)
            except MyProxyException as me:
                # Unsure if this works in standalone mode...
                cherrypy.log(str(me))
                cherrypy.log(str(serverKey))
                cherrypy.log(str(serverCert))
                invalidp = InvalidParameter("Impossible to retrieve proxy from %s for %s and hash %s" %
                                                (myproxyserver, kwargs['userdn'], userhash))
                setattr(invalidp, 'trace', str(me))
                raise invalidp

            else:
                if not re.match(RX_CERT, userproxy):
                    raise InvalidParameter("Retrieved malformed proxy from %s for %s and hash %s" %
                                                (myproxyserver, kwargs['userdn'], userhash))
        else:
            proxy = Proxy(defaultDelegation)
            userproxy = proxy.getProxyFilename()
        kwargs['userproxy'] = userproxy
        out = func(*args, **kwargs)
        return out
    return wrapped_func
