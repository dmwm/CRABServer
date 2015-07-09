import urllib
from httplib import HTTPException
from base64 import b64encode

from WMCore.Credential.Proxy import Proxy

from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.DataObjects.Result import Result
from TaskWorker.WorkerExceptions import TaskWorkerException

# We won't do anything if the proxy is shorted then 1 hour
# NB: in the PoC we had 24 hours, but does that make sense
#     for all possible commands, e.g. kill?
MINPROXYLENGTH = 60 * 60 * 1

class MyProxyLogon(TaskAction):

    def __init__(self, config, server, resturi, procnum=-1, myproxylen=MINPROXYLENGTH):
        TaskAction.__init__(self, config, server, resturi, procnum)
        self.myproxylen = myproxylen

    def execute(self, *args, **kwargs):
        result = None
        proxycfg = {'vo': kwargs['task']['tm_user_vo'],
                    'logger': self.logger,
                    'myProxySvr': self.config.Services.MyProxy,
                    'proxyValidity' : '144:0',
                    'min_time_left' : 36000, ## do we need this ? or should we use self.myproxylen? 
                    'userDN' : kwargs['task']['tm_user_dn'],
                    'group' : kwargs['task']['tm_user_group'] if kwargs['task']['tm_user_group'] else '',
                    'role' : kwargs['task']['tm_user_role'] if kwargs['task']['tm_user_role'] else '',
                    'server_key': self.config.MyProxy.serverhostkey,
                    'server_cert': self.config.MyProxy.serverhostcert,
                    'serverDN': self.config.MyProxy.serverdn,
                    'uisource': getattr(self.config.MyProxy, 'uisource', ''),
                    'credServerPath': self.config.MyProxy.credpath,
                    'myproxyAccount' : self.server['host'],
                    'cleanEnvironment' : getattr(self.config.MyProxy, 'cleanEnvironment', False)
                   }
        proxy = Proxy(proxycfg)
        userproxy = proxy.getProxyFilename(serverRenewer=True)
        proxy.logonRenewMyProxy()
        timeleft = proxy.getTimeLeft(userproxy)
        if timeleft is None or timeleft <= 0:
            msg = "Impossible to retrieve proxy from %s for %s." %(proxycfg['myProxySvr'], proxycfg['userDN'])
            raise TaskWorkerException(msg)
        else:
            kwargs['task']['user_proxy'] = userproxy
            result = Result(task=kwargs['task'], result='OK')
        return result
