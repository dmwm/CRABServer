"""
Ensure that TaskWorker has a valid VOMS proxy for this user via myproxy-logon
"""
import logging
from WMCore.Credential.Proxy import Proxy
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

    def __init__(self, config, crabserver, procnum=-1, myproxylen=MINPROXYLENGTH):
        TaskAction.__init__(self, config, crabserver, procnum)
        self.myproxylen = myproxylen

    def tryProxyLogon(self, proxycfg=None):
        """
        Utility function to allow trying with diffenent myproxy configurations.
        It tries to retrieve a valid proxy from myproxy using the configuration
        passed as argument. See WMCore.Credential.Proxy for configuration details.
        If successful returns the proxy filename and list of VOMS groups
        for later addition via voms-proxy-init. If not rises a TW exception.
        Note that logonRenewMyProxy() does not rise exceptions.
        """

        # WMCore proxy methods are awfully verbose, reduce logging level when using them
        with tempSetLogLevel(logger=self.logger, level=logging.ERROR):
            proxy = Proxy(proxycfg)
            userproxy = proxy.getProxyFilename(serverRenewer=True)  # this only returns a filename
            proxy.logonRenewMyProxy()  # this tries to create the proxy, but if it fails it does not rise
            usergroups = set(proxy.getAllUserGroups(userproxy))  # get VOMS groups from created proxy (if any)
            try:
                timeleft = proxy.getTimeLeft(userproxy)  # this is the way to tell if proxy creation succeeded
            except Exception as e:  # pylint: disable=broad-except
                errmsg = f"Exception raised inside Proxy.getTimeLeft: {e}"
                self.logger.error(errmsg)
                timeleft = 0
        errmsg = ''
        if timeleft is None or timeleft <= 0:
            errmsg = f"Impossible to retrieve proxy from {proxycfg['myProxySvr']} for {proxycfg['userDN']}."
        if timeleft < (5*24*3600):
            errmsg = f"Could not get a proxy valid for at least 5-days from {proxycfg['myProxySvr']} for {proxycfg['userDN']}."
        if errmsg:
            self.logger.error(errmsg)
            self.logger.error("Will try again in verbose mode")
            self.logger.error("===========PROXY ERROR START ==========================")
            with tempSetLogLevel(logger=self.logger, level=logging.DEBUG):
                proxy.logonRenewMyProxy()
            self.logger.error("===========PROXY ERROR END   ==========================")
            raise TaskWorkerException(errmsg)

        hoursleft = timeleft // 3600
        minutesleft = (timeleft % 3600) // 60
        self.logger.info('retrieved proxy lifetime in h:m: %d:%d', hoursleft, minutesleft)
        return (userproxy, usergroups)

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
                    'serverDN': 'dummy',  # this is only used inside WMCore/Proxy.py functions not used by CRAB
                    'uisource': getattr(self.config.MyProxy, 'uisource', ''),
                    'credServerPath': self.config.MyProxy.credpath,
                    'cleanEnvironment' : getattr(self.config.MyProxy, 'cleanEnvironment', False)
                   }
        self.logger.info("try to retrieve credential with DN hash as myproxy username")
        try:
            (userproxy, usergroups) = self.tryProxyLogon(proxycfg=proxycfg)
        except TaskWorkerException as ex:
            self.logger.error("proxy retrieval from %s failed with DN hash as credential name.",
                              proxycfg['myProxySvr'])
            raise TaskWorkerException(str(ex)) from ex
        #  minimal sanity check. Submission will fail if there's no group
        if not usergroups:
            raise TaskWorkerException(f"Could not retrieve VOMS groups list from {userproxy}")
        kwargs['task']['user_proxy'] = userproxy
        kwargs['task']['user_groups'] = usergroups
        self.logger.debug("Valid proxy for %s now in %s", proxycfg['userDN'], userproxy)
        result = Result(task=kwargs['task'], result='OK')

        return result
