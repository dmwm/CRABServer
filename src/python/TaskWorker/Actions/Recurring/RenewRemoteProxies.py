import os
import sys
import time
import json
import urllib
import logging
import traceback

import classad
import htcondor

import HTCondorUtils
from WMCore.Credential.Proxy import Proxy
from RESTInteractions import HTTPRequests
from TaskWorker.Actions.Recurring.BaseRecurringAction import BaseRecurringAction

class RenewRemoteProxies(BaseRecurringAction):
    pollingTime = 360 #minutes

    def _execute(self, resthost, resturi, config, task):
        renewer = CRAB3ProxyRenewer(config, resthost, resturi.replace("workflowdb","info"), self.logger)
        renewer.execute()

MINPROXYLENGTH = 60 * 60 * 24
QUERY_ATTRS = ['x509userproxyexpiration', 'CRAB_ReqName', 'ClusterId', 'ProcId', 'CRAB_UserDN', 'CRAB_UserVO', 'CRAB_UserGroup', 'CRAB_UserRole', 'JobStatus']

class CRAB3ProxyRenewer(object):

    def __init__(self, config, resthost, resturi, logger=None):
        if not logger:
            self.logger = logging.getLogger(__name__)
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger = logger

        self.resturi = resturi
        self.resthost = resthost
        self.config = config
        self.pool = ''
        self.schedds = []

        htcondor.param['TOOL_DEBUG'] = 'D_FULLDEBUG D_SECURITY'
        if 'CRAB3_DEBUG' in os.environ and hasattr(htcondor, 'enable_debug'):
            htcondor.enable_debug()

    def get_backendurls(self):
        self.logger.info("Querying server %s for HTCondor schedds and pool names." % self.resturi)
        server = HTTPRequests(self.resthost, self.config.TaskWorker.cmscert, self.config.TaskWorker.cmskey, retry = 2)
        result = server.get(self.resturi, data={'subresource':'backendurls'})[0]['result'][0]
        self.pool = str(result['htcondorPool'])
        self.schedds = [str(i) for i in result['htcondorSchedds']]
        self.logger.info("Resulting pool %s; schedds %s" % (self.pool, ",".join(self.schedds)))

    def get_proxy(self, ad):
        result = None
        vo = 'cms'
        group = ''
        role = ''
        if 'CRAB_UserVO' in ad and ad['CRAB_UserVO']:
            vo = ad['CRAB_UserVO']
        if 'CRAB_UserGroup' in ad and ad['CRAB_UserGroup'] and ad['CRAB_UserGroup'] != classad.Value.Undefined:
            group = ad['CRAB_UserGroup']
        if 'CRAB_UserRole' in ad and ad['CRAB_UserRole'] and ad['CRAB_UserRole'] != classad.Value.Undefined:
            role = ad['CRAB_UserRole']
        print vo, group, role
        proxycfg = {'vo': vo,
                    'logger': self.logger,
                    'myProxySvr': self.config.Services.MyProxy,
                    'myproxyAccount': self.config.TaskWorker.resturl,
                    'proxyValidity' : '144:0',
                    'min_time_left' : MINPROXYLENGTH, ## do we need this ? or should we use self.myproxylen? 
                    'userDN' : ad['CRAB_UserDN'],
                    'group' : group,
                    'role' : role,
                    'server_key': self.config.MyProxy.serverhostkey,
                    'server_cert': self.config.MyProxy.serverhostcert,
                    'serverDN': self.config.MyProxy.serverdn,
                    'uisource': getattr(self.config.MyProxy, 'uisource', ''),
                    'credServerPath': self.config.MyProxy.credpath,
                    'cleanEnvironment' : getattr(self.config.MyProxy, 'cleanEnvironment', False)}
        proxy = Proxy(proxycfg)
        userproxy = proxy.getProxyFilename(serverRenewer=True)
        proxy.logonRenewMyProxy()
        timeleft = proxy.getTimeLeft(userproxy)
        if timeleft is None or timeleft <= 0:
            self.logger.error("Impossible to retrieve proxy from %s for %s." %(proxycfg['myProxySvr'], proxycfg['userDN']))
            raise Exception("Failed to retrieve proxy.")
        return userproxy

    def renew_proxy(self, schedd, ad, proxy):
        now = time.time()
        self.logger.info("Renewing proxy for task %s." % ad['CRAB_ReqName'])
        if not hasattr(schedd, 'refreshGSIProxy'):
            raise NotImplementedError()
        with HTCondorUtils.AuthenticatedSubprocess(proxy) as (parent, rpipe):
            if not parent:
                lifetime = schedd.refreshGSIProxy(ad['ClusterId'], ad['ProcID'], proxy, -1)
                schedd.edit(['%s.%s' % (ad['ClusterId'], ad['ProcId'])], 'x509userproxyexpiration', str(int(now+lifetime)))
        results = rpipe.read()
        if results != "OK":
            raise Exception("Failure when renewing HTCondor task proxy: '%s'" % results)

    def execute_schedd(self, schedd_name, collector):
        self.logger.info("Updating tasks in schedd %s" % schedd_name)
        self.logger.info("Trying to locate schedd.")
        schedd_ad = collector.locate(htcondor.DaemonTypes.Schedd, schedd_name)
        self.logger.info("Schedd found at %s" % schedd_ad['MyAddress'])
        schedd = htcondor.Schedd(schedd_ad)
        self.logger.info("Querying schedd for CRAB3 tasks.")
        task_ads = list(schedd.xquery('JobStatus =!= 4 && TaskType =?= "ROOT" && CRAB_HC =!= "True"', QUERY_ATTRS))
        self.logger.info("There were %d tasks found." % len(task_ads))
        ads = {}
        now = time.time()
        for ad in task_ads:
            if 'x509userproxyexpiration' in ad:
                lifetime = ad['x509userproxyexpiration'] - now
                if lifetime > MINPROXYLENGTH:
                    self.logger.info("Skipping refresh of proxy for task %s because it still has a lifetime of %.1f hours." % (ad['CRAB_ReqName'], lifetime/3600.0))
                    continue
            user = ad['CRAB_UserDN']
            vo = 'cms'
            group = ''
            role = ''
            if 'CRAB_UserVO' in ad and ad['CRAB_UserVO']:
                vo = ad['CRAB_UserVO']
            if 'CRAB_UserGroup' in ad and ad['CRAB_UserGroup'] and ad['CRAB_UserGroup'] != classad.Value.Undefined:
                group = ad['CRAB_UserGroup']
            if 'CRAB_UserRole' in ad and ad['CRAB_UserRole'] and ad['CRAB_UserRole'] != classad.Value.Undefined:
                role = ad['CRAB_UserRole']
            key = (user, vo, group, role)
            ad_list = ads.setdefault(key, [])
            ad_list.append(ad)

        for key, ad_list in ads.items():
            self.logger.info("Retrieving proxy for %s" % str(key))
            try:
                proxyfile = self.get_proxy(ad_list[0])
            except Exception:
                self.logger.exception("Failed to retrieve proxy.  Skipping user")
                continue
            for ad in ad_list:
                try:
                    self.renew_proxy(schedd, ad, proxyfile)
                except NotImplementedError:
                    raise
                except Exception:
                    self.logger.exception("Failed to renew proxy for task %s due to exception." % ad['CRAB_ReqName'])

    def execute(self):
        self.get_backendurls()
        collector = htcondor.Collector(self.pool)
        for schedd_name in self.schedds:
            try:
                self.execute_schedd(schedd_name, collector)
                self.logger.info("Done updating proxies for schedd %s" % schedd_name)
            except NotImplementedError:
                raise
            except Exception:
                self.logger.exception("Unable to update all proxies for schedd %s" % schedd_name)

if __name__ == '__main__':
    """ Simple main to execute the action standalon. You just need to set the task worker environment.
        The main is set up to work with the production task worker. If you want to use it on your own
        instance you need to change resthost, resturi, and twconfig. For example:
            resthost = 'mmascher-dev6.cern.ch'
            resturi = '/crabserver/dev/info'
            twconfig = '/data/srv/TaskManager/TaskWorkerConfig.py'
    """
    resthost = 'cmsweb.cern.ch'
    resturi = '/crabserver/prod/info'
    twconfig = '/data/srv/TaskManager/current/TaskWorkerConfig.py'

    logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", datefmt="%a, %d %b %Y %H:%M:%S %Z(%z)")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    from WMCore.Configuration import loadConfigurationFile
    config = loadConfigurationFile(twconfig)

    pr = CRAB3ProxyRenewer(config, resthost, resturi, logger)
    pr.execute()
