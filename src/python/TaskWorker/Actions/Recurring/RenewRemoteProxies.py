# pylint: disable=C0103, W0703, R0912, R0914, R0915
""" Renew user proxies on schedulers """
import os
import sys
import time
import logging

import classad
import htcondor

from WMCore.Credential.Proxy import Proxy
from WMCore.Configuration import loadConfigurationFile

from RESTInteractions import CRABRest
from ServerUtilities import  tempSetLogLevel
from TaskWorker.MasterWorker import getRESTParams

from TaskWorker.Actions.Recurring.BaseRecurringAction import BaseRecurringAction

class RenewRemoteProxies(BaseRecurringAction):
    """ need a doc string here """
    pollingTime = 60 * 12 #minutes

    def _execute(self, config, task):  # pylint: disable=unused-argument
        """ need a doc string here """
        renewer = CRAB3ProxyRenewer(config, self.logger)
        renewer.execute()

MINPROXYLENGTH = 60 * 60 * 24
QUERY_ATTRS = ['x509userproxyexpiration', 'CRAB_ReqName', 'ClusterId', 'ProcId',
               'CRAB_UserHN', 'CRAB_UserDN', 'CRAB_UserVO', 'CRAB_UserGroup',
               'CRAB_UserRole', 'JobStatus']

class CRAB3ProxyRenewer():
    """ need a doc string here """

    def __init__(self, config, logger=None):
        if not logger:
            self.logger = logging.getLogger(__name__)
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.DEBUG)
        else:
        # do not use BaseRecurringAction logger but create a new logger
        # which writes to config.TaskWorker.logsDir/taks/recurring/RenewRemoveProxies_YYMMDD-HHMM.log
            self.logger = logging.getLogger('RenewRemoteProxies')
            logDir = config.TaskWorker.logsDir + '/tasks/recurring/'
            if not os.path.exists(logDir):
                os.makedirs(logDir)
            timeStamp = time.strftime('%y%m%d-%H%M', time.localtime())
            logFile = 'RenewRemoveProxies_' + timeStamp + '.log'
            handler = logging.FileHandler(logDir + logFile)
            formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(module)s:%(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        self.config = config
        self.pool = ''
        self.schedds = []
        self.restHost = None
        self.dbInstance = None

        tokenDir = getattr(self.config.TaskWorker, 'SEC_TOKEN_DIRECTORY', None)
        htcondor.param['SEC_TOKEN_DIRECTORY'] = tokenDir

        htcondor.param['TOOL_DEBUG'] = 'D_FULLDEBUG D_SECURITY'
        if 'CRAB3_DEBUG' in os.environ and hasattr(htcondor, 'enable_debug'):
            htcondor.enable_debug()

    def get_backendurls(self):
        """ need a doc string here """
        # need to deal with the fact that TaskWorkerConfig may only specify a name instance
        self.restHost, self.dbInstance = getRESTParams(self.config, self.logger)
        self.logger.info("Querying server %s for HTCondor schedds and pool names.", self.restHost)
        crabserver = CRABRest(self.restHost, self.config.TaskWorker.cmscert, self.config.TaskWorker.cmskey,
                              retry=2, userAgent='CRABTaskWorker')
        crabserver.setDbInstance(self.dbInstance)
        result = crabserver.get(api='info', data={'subresource':'backendurls'})[0]['result'][0]
        self.pool = str(result['htcondorPool'])
        self.schedds = [str(i) for i in result['htcondorSchedds']]
        self.logger.info("Resulting pool %s; schedds %s", self.pool, ",".join(self.schedds))

    def get_proxy_from_MyProxy(self, ad):
        """ need a doc string here """
        vo = 'cms'
        group = ''
        role = ''
        if 'CRAB_UserVO' in ad and ad['CRAB_UserVO']:
            vo = ad['CRAB_UserVO']
        if 'CRAB_UserGroup' in ad and ad['CRAB_UserGroup'] and ad['CRAB_UserGroup'] is not classad.Value.Undefined:
            group = ad['CRAB_UserGroup']
        if 'CRAB_UserRole' in ad and ad['CRAB_UserRole'] and ad['CRAB_UserRole'] is not classad.Value.Undefined:
            role = ad['CRAB_UserRole']
        username = ad['CRAB_UserHN']
        proxycfg = {'vo': vo,
                    'logger': self.logger,
                    'myProxySvr': self.config.Services.MyProxy,
                    'proxyValidity' : '144:0',
                    'min_time_left' : MINPROXYLENGTH, ## do we need this ? or should we use self.myproxylen?
                    'userDN' : ad['CRAB_UserDN'],
                    'userName' : username + '_CRAB',
                    'group' : group,
                    'role' : role,
                    'server_key': self.config.MyProxy.serverhostkey,
                    'server_cert': self.config.MyProxy.serverhostcert,
                    'serverDN': 'dummy',  # this is only used inside WMCore/Proxy.py functions not used by CRAB
                    'uisource': getattr(self.config.MyProxy, 'uisource', ''),
                    'credServerPath': self.config.MyProxy.credpath,
                    'cleanEnvironment' : getattr(self.config.MyProxy, 'cleanEnvironment', False)}

        proxy = Proxy(proxycfg)
        userproxy = proxy.getProxyFilename(serverRenewer=True)
        # try first with new username_CRAB
        with tempSetLogLevel(logger=self.logger, level=logging.ERROR):
            proxy.logonRenewMyProxy()
            timeleft = proxy.getTimeLeft(userproxy)
        if not timeleft or timeleft <= 0:
            # if that fails, try with old fashioned DN hash
            del proxycfg['userName']
            proxy = Proxy(proxycfg)
            with tempSetLogLevel(logger=self.logger, level=logging.ERROR):
                proxy.logonRenewMyProxy()
                timeleft = proxy.getTimeLeft(userproxy)
        if timeleft is None or timeleft <= 0:
            self.logger.error("Impossible to retrieve proxy from %s for %s.", proxycfg['myProxySvr'], proxycfg['userDN'])
            self.logger.error("repeat the command in verbose mode")
            proxycfg['userName'] = username + '_CRAB'
            proxy = Proxy(proxycfg)
            proxy.logonRenewMyProxy()
            raise Exception("Failed to retrieve proxy.")
        return userproxy

    def push_new_proxy_to_schedd(self, schedd, ad, proxy):
        """ need a doc string here """
        if not hasattr(schedd, 'refreshGSIProxy'):
            raise NotImplementedError()
        try:
            schedd.refreshGSIProxy(ad['ClusterId'], ad['ProcID'], proxy, -1)
        except Exception as e:
            raise Exception(f"Failure when renewing HTCondor task proxy: {e}") from e

    def execute_schedd(self, schedd_name, collector):
        """ need a doc string here """

        self.logger.info("Updating tasks in schedd %s", schedd_name)
        self.logger.debug("Trying to locate schedd.")
        schedd_ad = collector.locate(htcondor.DaemonTypes.Schedd, schedd_name)
        self.logger.debug("Schedd found at %s", schedd_ad['MyAddress'])
        schedd = htcondor.Schedd(schedd_ad)
        self.logger.debug("Querying schedd for CRAB3 tasks.")
        task_ads = list(schedd.xquery('TaskType =?= "ROOT" && CRAB_HC =!= "True"', QUERY_ATTRS))
        self.logger.info("There were %d tasks found.", len(task_ads))
        ads = {}
        now = time.time()
        for ad in task_ads:
            ## TO DO we should detect the TW is shutting down and exit this loop
            if 'x509userproxyexpiration' in ad:
                lifetime = ad['x509userproxyexpiration'] - now
                if lifetime > MINPROXYLENGTH:
                    msg = f"Skipping refresh of proxy for task {ad['CRAB_ReqName']} because "
                    msg += "it still has a lifetime of {int(lifetime/3600)} hours."
                    self.logger.info(msg)
                    continue
            user = ad['CRAB_UserDN']
            vo = 'cms'
            group = ''
            role = ''
            if 'CRAB_UserVO' in ad and ad['CRAB_UserVO']:
                vo = ad['CRAB_UserVO']
            if 'CRAB_UserGroup' in ad and ad['CRAB_UserGroup'] and ad['CRAB_UserGroup'] is not classad.Value.Undefined:
                group = ad['CRAB_UserGroup']
            if 'CRAB_UserRole' in ad and ad['CRAB_UserRole'] and ad['CRAB_UserRole'] is not classad.Value.Undefined:
                role = ad['CRAB_UserRole']
            key = (user, vo, group, role)
            ad_list = ads.setdefault(key, [])
            ad_list.append(ad)

        for key, ad_list in ads.items():
            self.logger.info("Retrieving proxy for %s", str(key))
            try:
                proxyfile = self.get_proxy_from_MyProxy(ad_list[0])
            except Exception:
                self.logger.error("Failed to retrieve proxy.  Skipping user %s", key[0])
                tasks = '\n\t'.join((ad['CRAB_ReqName'] for ad in ad_list))
                self.logger.error("Will not update proxy for tasks:\n\t%s", tasks)
                continue
            for ad in ad_list:
                try:
                    self.push_new_proxy_to_schedd(schedd, ad, proxyfile)
                except NotImplementedError:
                    raise
                except Exception:
                    msg = f"Failed to push new proxy to schedd for task {ad['CRAB_ReqName']} due to exception."
                    self.logger.exception(msg)
                    continue
                self.logger.debug("Updated proxy pushed to schedd for task %s", ad['CRAB_ReqName'])

    def remove_handler(self):
        """ need a doc string here """
        handlers = self.logger.handlers[:]
        for file_handler in handlers:
            file_handler.close()
            self.logger.removeHandler(file_handler)

    def execute(self):
        """ need a doc string here """
        self.get_backendurls()
        collector = htcondor.Collector(self.pool)
        for schedd_name in self.schedds:
            try:
                self.execute_schedd(schedd_name, collector)
                self.logger.info("Done updating proxies for schedd %s", schedd_name)
            except NotImplementedError:
                raise
            except Exception:
                self.logger.exception("Unable to update all proxies for schedd %s", schedd_name)
        self.remove_handler()

def main():
    """ Simple main to execute the action standalon. You just need to set the task worker environment.
        The main is set up to work with the production task worker. If you want to use it on your own
        instance you need to point to a modified dtwconfig. Default is:
            twconfig = '/data/srv/TaskManager/TaskWorkerConfig.py'
    """
    twconfig = '/data/srv/TaskManager/current/TaskWorkerConfig.py'

    logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", datefmt="%a, %d %b %Y %H:%M:%S %Z(%z)")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    config = loadConfigurationFile(twconfig)

    pr = CRAB3ProxyRenewer(config, logger)
    pr.execute()

if __name__ == '__main__':
    main()
