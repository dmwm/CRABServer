
import logging
from TaskWorker.DataObjects.Result import Result
from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.WorkerExceptions import TaskWorkerException
from ServerUtilities import tempSetLogLevel

# We won't do anything if the proxy is shorted then 1 hour
# NB: in the PoC we had 24 hours, but does that make sense
#     for all possible commands, e.g. kill?
MINPROXYLENGTH = 60 * 60 * 1

class MyProxyLogon(TaskAction):
    """ Retrieves the user proxy from myproxy
    """

    def __init__(self, config, server, resturi, procnum=-1, myproxylen=MINPROXYLENGTH):
        TaskAction.__init__(self, config, server, resturi, procnum)
        self.myproxylen = myproxylen

    def tryProxyLogon(self, proxycfg=None):
        """
        Utility function to allow trying with diffenent myproxy configurations.
        It tries to retrieve a valid proxy from myproxy using the configuration
        passed as argument. See WMCore.Credential.Proxy for configuration details.
        If successful returns the proxy filename and list of VOMS groups
        for later addition via voms-proxy-init. If not rises a TW exception
        Note that logonRenewMyProxy() does not rise exceptions.
        """
        from WMCore.Credential.Proxy import Proxy

        # WMCore proxy methods are awfully verbose, reduce logging level when using them
        with tempSetLogLevel(logger=self.logger, level=logging.ERROR):
            proxy = Proxy(proxycfg)
            userproxy = proxy.getProxyFilename(serverRenewer=True)
            usergroups = set(proxy.getAllUserGroups(userproxy))
            proxy.logonRenewMyProxy()
            timeleft = proxy.getTimeLeft(userproxy)

        if timeleft is None or timeleft <= 0:
            msg = "Impossible to retrieve proxy from %s for %s." % (proxycfg['myProxySvr'], proxycfg['userDN'])
            self.logger.error(msg)
            self.logger.error("Will try again in verbose mode")
            self.logger.error("===========PROXY ERROR START ==========================")
            with tempSetLogLevel(logger=self.logger, level=logging.DEBUG):
                proxy.logonRenewMyProxy()
            self.logger.error("===========PROXY ERROR END   ==========================")
            raise TaskWorkerException(msg)

        return (userproxy, usergroups)

    def execute(self, *args, **kwargs):
        result = None
        proxycfg = {'vo': kwargs['task']['tm_user_vo'],
                    'logger': self.logger,
                    'myProxySvr': self.config.Services.MyProxy,
                    'proxyValidity' : '144:0',
                    'min_time_left' : 36000, ## do we need this ? or should we use self.myproxylen?
                    'userName' : kwargs['task']['tm_username'] + '_CRAB',
                    'userDN' : kwargs['task']['tm_user_dn'],
                    'group' : kwargs['task']['tm_user_group'] if kwargs['task']['tm_user_group'] else '',
                    'role' : kwargs['task']['tm_user_role'] if kwargs['task']['tm_user_role'] else '',
                    'server_key': self.config.MyProxy.serverhostkey,
                    'server_cert': self.config.MyProxy.serverhostcert,
                    'serverDN': 'dummy',  # this is only used inside WMCore/Proxy.py functions not used by CRAB
                    'uisource': getattr(self.config.MyProxy, 'uisource', ''),
                    'credServerPath': self.config.MyProxy.credpath,
                    'cleanEnvironment' : getattr(self.config.MyProxy, 'cleanEnvironment', False)
                   }
        try:
            # try first to retrieve credential with login name = <username>_CRAB
            (userproxy, usergroups) = self.tryProxyLogon(proxycfg=proxycfg)
        except TaskWorkerException:
            self.logger.error("proxy retrieval from %s failed with login name %s.",
                              proxycfg['myProxySvr'], proxycfg['userName'])
            self.logger.error("will try with old-style DN hash")
            del proxycfg['userName']
            try:
                (userproxy, usergroups) = self.tryProxyLogon(proxycfg=proxycfg)
            except TaskWorkerException:
                self.logger.error("proxy retrieval from %s failed with DN hash as credential name.",
                                  proxycfg['myProxySvr'])
                self.logger.error("will try with old-style DN hash adding REST host name")
                proxycfg['myproxyAccount'] = self.server['host']
                (userproxy, usergroups) = self.tryProxyLogon(proxycfg=proxycfg)
        #  minimal sanity check. Submission will fail if there's no group
        if not usergroups:
            raise TaskWorkerException('Could not retrieve VOMS groups list from %s' % userproxy)
        kwargs['task']['user_proxy'] = userproxy
        kwargs['task']['user_groups'] = usergroups
        self.logger.debug("Valid proxy for %s now in %s", proxycfg['userDN'], userproxy)
        result = Result(task=kwargs['task'], result='OK')

        return result
