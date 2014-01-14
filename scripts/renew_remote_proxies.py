import os
import sys
import time
import json
import urllib
import logging
import optparse
import traceback

import htcondor

import CRABInterface.HTCondorUtils as HTCondorUtils
from WMCore.Configuration import loadConfigurationFile
from WMCore.Credential.Proxy import Proxy
from RESTInteractions import HTTPRequests

MINPROXYLENGTH = 60 * 60 * 24

QUERY_ATTRS = ['x509userproxyexpiration', 'CRAB_ReqName', 'ClusterId', 'ProcId', 'CRAB_UserDN', 'CRAB_UserVO', 'CRAB_UserGroup', 'CRAB_UserRole', 'JobStatus']

class CRAB3ProxyRenewer(object):

    def __init__(self, config):
        self.logger = logging.getLogger()
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)

        self.resturl = config.TaskWorker.resturl
        self.config = config
        self.pool = ''
        self.schedds = []

        htcondor.param['TOOL_DEBUG'] = 'D_FULLDEBUG D_SECURITY'
        if hasattr(htcondor, 'enable_debug'):
            htcondor.enable_debug()

    def get_backendurls(self, service="/crabserver/dev/info"):
        self.logger.info("Querying server %s for HTCondor schedds and pool names." % self.resturl)
        server = HTTPRequests(self.resturl, self.config.TaskWorker.cmscert, self.config.TaskWorker.cmskey)
        result = server.get('/crabserver/dev/info', data={'subresource':'backendurls'})[0]['result'][0]
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
        if 'CRAB_UserGroup' in ad and ad['CRAB_UserGroup']:
            group = ad['CRAB_UserGroup']
        if 'CRAB_UserRole' in ad and ad['CRAB_UserRole']:
            role = ad['CRAB_UserRole']
        print vo, group, role
        proxycfg = {'vo': vo,
                    'logger': self.logger,
                    'myProxySvr': self.config.Services.MyProxy,
                    'proxyValidity' : '144:0',
                    'min_time_left' : MINPROXYLENGTH, ## do we need this ? or should we use self.myproxylen? 
                    'userDN' : ad['CRAB_UserDN'],
                    'group' : group,
                    'role' : role,
                    'server_key': self.config.MyProxy.serverhostkey,
                    'server_cert': self.config.MyProxy.serverhostcert,
                    'serverDN': self.config.MyProxy.serverdn,
                    'uisource': self.config.MyProxy.uisource,
                    'credServerPath': self.config.MyProxy.credpath,}
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
        task_ads = schedd.query('JobStatus =!= 4 && TaskType =?= "ROOT"', QUERY_ATTRS)
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
            if 'CRAB_UserGroup' in ad and ad['CRAB_UserGroup']:
                group = ad['CRAB_UserGroup']
            if 'CRAB_UserRole' in ad and ad['CRAB_UserRole']:
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

def main():

    parser = optparse.OptionParser()
    parser.add_option("-c", "--config", dest="config", help="TaskWorker configuration file.")
    opts, args = parser.parse_args()

    configuration = loadConfigurationFile( os.path.abspath(opts.config) )
    renewer = CRAB3ProxyRenewer(configuration)
    renewer.execute()

if __name__ == '__main__':
    main()

