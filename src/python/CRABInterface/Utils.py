import logging
import copy
import traceback
import os
from subprocess import call
from collections import namedtuple
from time import mktime, gmtime
import inspect

from WMCore.REST.Error import ExecutionError, InvalidParameter
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import addSiteWildcards
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Credential.Proxy import Proxy

"""
The module contains some utility functions used by the various modules of the CRAB REST interface
"""

CMSSitesCache = namedtuple("CMSSitesCache", ["cachetime", "sites"])

#These parameters are set in the globalinit (called in RESTBaseAPI)
serverCert = None
serverKey = None
serverDN = None
uiSource = None
credServerPath = '/tmp'

def globalinit(serverkey, servercert, serverdn, uisource, credpath):
    global serverCert, serverKey, serverDN, uiSource, credServerPath
    serverCert, serverKey, serverDN, uiSource, credServerPath = servercert, serverkey, serverdn, uisource, credpath

def execute_command( command, logger, timeout ):
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



def conn_handler(services):
    """
    Decorator to be used among REST resources to optimize connections to other services
    as CouchDB and SiteDB, PhEDEx, WMStats monitoring

    arg str list services: list of string telling which service connections
                           should be started; currently availables are
                           'monitor' and 'asomonitor'.
    """
    def wrap(func):
        def wrapped_func(*args, **kwargs):
            if 'sitedb' in services and (not args[0].allCMSNames.sites or (args[0].allCMSNames.cachetime+1800 < mktime(gmtime()))):
                args[0].allCMSNames = CMSSitesCache(sites=SiteDBJSON().getAllCMSNames(), cachetime=mktime(gmtime()))
                if hasattr(args[0], 'wildcardKeys') and hasattr(args[0], 'wildcardSites'):
                    addSiteWildcards(args[0].wildcardKeys, args[0].allCMSNames.sites, args[0].wildcardSites)
            if 'phedex' in services and not args[0].phedex:
                args[0].phedex = PhEDEx(responseType='xml', dict=args[0].phedexargs)
            return func(*args, **kwargs)
        return wrapped_func
    return wrap

def retriveUserCert(clean=True):
    def wrap(func):
        def wrapped_func(*args, **kwargs):
            logger = logging.getLogger("CRABLogger.Utils")
            myproxyserver = "myproxy.cern.ch"
            userdn = kwargs['userdn']
            defaultDelegation = { 'vo': 'cms',
                                  'logger': logger,
                                  'myProxySvr': 'myproxy.cern.ch',
                                  'proxyValidity' : '192:00',
                                  'min_time_left' : 36000,
                                  'userDN' : userdn,
                                  'server_key': serverKey,
                                  'server_cert': serverCert,
                                  'serverDN': serverDN,
                                  'uisource': uiSource,
                                  'credServerPath': credServerPath,}
            #print defaultDelegation
            timeleftthreshold = 60 * 60 * 24
            old_ld_library_path = os.environ['LD_LIBRARY_PATH']
            os.environ['LD_LIBRARY_PATH'] = ''
            try:
                proxy = Proxy(defaultDelegation)
                if serverCert:
                    userproxy = proxy.getProxyFilename(serverRenewer=True)
                else:
                    userproxy = proxy.getProxyFilename()
                timeleft = proxy.getTimeLeft()
                if timeleft < timeleftthreshold:
                    proxy.logonRenewMyProxy()
                    timeleft = proxy.getTimeLeft( userproxy )
                    if timeleft is None or timeleft <= 0:
                        raise InvalidParameter("Impossible to retrieve proxy from %s for %s." %(defaultDelegation['myProxySvr'], defaultDelegation['userDN']))

                    logger.debug("User proxy file path: %s" % userproxy)
            finally:
                os.environ['LD_LIBRARY_PATH'] = old_ld_library_path
            kwargs['userproxy'] = userproxy
            out = func(*args, **kwargs)
            if clean:
                logger.debug("Rimuovo %s" % clean)
                os.remove(userproxy)
            return out
        return wrapped_func
    return wrap

