"""
Resolve CRIC-related Sites info.
"""
# pylint: disable=too-many-branches

import re
from TaskWorker.DataObjects.Result import Result
from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.WorkerExceptions import ConfigException, TaskWorkerException
from TaskWorker.WorkerUtilities import CRICService

class SiteInfoResolver(TaskAction):
    """
    Given a task definition, resolve site white/black list with CRIC-related sites informations.
    """

    def __init__(self, config, crabserver, procnum=-1):
        """ need a comment line here """
        TaskAction.__init__(self, config, crabserver, procnum)
        with config.TaskWorker.envForCMSWEB:
            self.resourceCatalog = CRICService(logger=self.logger, configDict={"cacheduration": 1, "pycurl": True, "usestalecache": True})

    def isGlobalBlacklistIgnored(self, kwargs):
        """ Determine wether the user wants to ignore the globalblacklist
        """

        return kwargs['task']['tm_ignore_global_blacklist'] == 'T'

    def execute(self, *args, **kwargs):
        """ ... """
        
        task = kwargs['task']
        global_blacklist = set(self.loadJSONFromFileInScratchDir('blacklistedSites.txt'))
        self.logger.debug("CRAB site blacklist: %s", list(global_blacklist))
        # This is needed for Site Metrics
        # It should not block any site for Site Metrics and if needed for other activities
        # self.config.TaskWorker.ActivitiesToRunEverywhere = ['hctest', 'hcdev']
        # The other case where the blacklist is ignored is if the user sset this explicitly in his configuration
        if self.isGlobalBlacklistIgnored(kwargs) or (hasattr(self.config.TaskWorker, 'ActivitiesToRunEverywhere') and \
                   task['tm_activity'] in self.config.TaskWorker.ActivitiesToRunEverywhere):
            global_blacklist = set()
            self.logger.debug("Ignoring the CRAB site blacklist.")

        ### Deprecated codes BELOW ###:
        ### might still usefull as a reminder, if we decided to support storage site checking/resolving again.
        ### bannedOutDestinations = self.crabserver.get(api='info', data={'subresource': 'bannedoutdest'})[0]['result'][0]
        ### self._checkASODestination(kwargs['task']['tm_asyncdest'], bannedOutDestinations)

        task['tm_site_whitelist'] = self._expandSites(task['tm_site_whitelist'])
        task['tm_site_blacklist'] = self._expandSites(task['tm_site_blacklist'])
        if 'resubmit_site_whitelist' in task and task['resubmit_site_whitelist']:
            task['resubmit_site_whitelist']  = self._expandSites(task['resubmit_site_whitelist'])
        if 'resubmit_site_blacklist' in task and task['resubmit_site_blacklist']:
            task['resubmit_site_blacklist']  = self._expandSites(task['resubmit_site_blacklist'])

        self.logger.debug("Site whitelist: %s", list(task['tm_site_whitelist']))
        self.logger.debug("Site blacklist: %s", list(task['tm_site_blacklist']))

        # now turn lists into sets to do intersection
        siteWhitelist = set(task['tm_site_whitelist'])
        siteBlacklist = set(task['tm_site_blacklist'])

        if siteWhitelist & global_blacklist:
            msg = f"The following sites from the user site whitelist are blacklisted by the CRAB server: {list(siteWhitelist & global_blacklist)}."
            msg += " Since the CRAB server blacklist has precedence, these sites are not considered in the user whitelist."
            self.uploadWarning(msg, task['user_proxy'], task['tm_taskname'])
            self.logger.warning(msg)

        if siteBlacklist & siteWhitelist:
            msg = f"The following sites appear in both the user site blacklist and whitelist: {list(siteBlacklist & siteWhitelist)}."
            msg += " Since the whitelist has precedence, these sites are not considered in the blacklist."
            self.uploadWarning(msg, task['user_proxy'], task['tm_taskname'])
            self.logger.warning(msg)

        try:
            all_possible_processing_sites = (
                set(self.resourceCatalog.getAllPSNs()) - global_blacklist
            )
            task['all_possible_processing_sites'] = list(all_possible_processing_sites)
        except Exception as ex:
            msg = "The CRAB3 server backend could not contact the Resource Catalog to get the list of all CMS sites."
            msg += " This could be a temporary Resource Catalog glitch."
            msg += " Please try to submit a new task (resubmit will not work)"
            msg += " and contact the experts if the error persists."
            msg += f"\nError reason: {ex}"
            raise TaskWorkerException(msg) from ex

        return Result(task=task, result=(all_possible_processing_sites, args[0]))

    def _expandSites(self, sites, pnn=False):
        """Check if there are sites cotaining the '*' wildcard and convert them in the corresponding list
           Raise exception if any wildcard site does expand to an empty list
           note that all*names.sites come from an HTTP query to CRIC which returns JSON and thus are unicode
            Args:
                sites (list): A list of site names, possibly containing '*' wildcards e.g T2_US*, T2_UK*
            Returns:
                list: A list of expanded site names with no wildcards.
        """
        res = set()
        for site in sites:
            if '*' in site:
                sitere = re.compile(site.replace('*', '.*'))
                expanded = [str(s) for s in (self.resourceCatalog.getAllPhEDExNodeNames() if pnn else self.resourceCatalog.getAllPSNs()) if sitere.match(s)]
                self.logger.debug("Site %s expanded to %s during validate", site, expanded)
                if not expanded:
                    raise ConfigException('Remote output data site not valid, Cannot expand site %s to anything' % site)
                res = res.union(expanded)
            else:
                self._checkSite(site, pnn) # pylint: disable=protected-access
                res.add(site)
        return list(res)

    def _checkASODestination(self, site, bannedOutDestinations=None):
        """ Check that the ASO destination is correct and not among the banned ones
        """
        if bannedOutDestinations is None:
            bannedOutDestinations = []

        self._checkSite(site, pnn=True)
        expandedBannedDestinations = self._expandSites(bannedOutDestinations, pnn=True)
        if site in expandedBannedDestinations:
            raise ConfigException("Remote output data site is banned, The output site you specified in the Site.storageSite parameter (%s) is blacklisted (banned sites: %s)" % (site, bannedOutDestinations))

    def _checkSite(self, site, pnn=False):
        """ Check a single site like T2_IT_Something against known CMS site names
        """
        sites = self.resourceCatalog.getAllPhEDExNodeNames() if pnn else self.resourceCatalog.getAllPSNs()
        if site not in sites:
            raise ConfigException('A site name %s that user specified is not in the list of known CMS %s' % (site, 'Storage nodes' if pnn else 'Processing Site Names'))


if __name__ == '__main__':
    ###
    # Usage: (Inside TaskWorker container)
    # > export PYTHONPATH="$PYTHONPATH:/data/srv/current/lib/python/site-packages"
    # > python3 SiteInfoResolver.py
    ###

    import logging
    from TaskWorker.WorkerUtilities import CRICService # pylint: disable=W0404
    from WMCore.Configuration import loadConfigurationFile
    from ServerUtilities import newX509env
    test_config = loadConfigurationFile('/data/srv/TaskManager/cfg/TaskWorkerConfig.py')
    envForCMSWEB = newX509env(X509_USER_CERT=test_config.TaskWorker.cmscert, X509_USER_KEY=test_config.TaskWorker.cmskey)
    logging.basicConfig(level=logging.DEBUG)

    MOCK_BANNED_OUT_DESTINATIONS = ['T2_UK_SGrid_Bristol', 'T2_US_MIT']
    with envForCMSWEB:
        resolver = SiteInfoResolver(test_config, crabserver=None)
        assert resolver._checkSite('T2_US_Florida', pnn=False) is None, 'Site T2_US_Florida is valid' # pylint: disable=protected-access
        try:
            resolver._checkSite('T2_TH_Bangkok', pnn=False) # pylint: disable=protected-access
        except ConfigException as exc:
            assert str(exc) == 'A site name T2_TH_Bangkok that user specified is not in the list of known CMS Processing Site Names', 'Site T2_TH_Bangkok are not existing and should be invalid'
        assert resolver._checkASODestination('T2_US_Florida', MOCK_BANNED_OUT_DESTINATIONS) is None, 'Site: T2_US_Florida, Should not be banned' # pylint: disable=protected-access
        try:
            resolver._checkASODestination('T2_UK_SGrid_Bristol', MOCK_BANNED_OUT_DESTINATIONS) # pylint: disable=protected-access
        except ConfigException as exc:
            assert str(exc) == 'Remote output data site is banned, The output site you specified in the Site.storageSite parameter (T2_UK_SGrid_Bristol) is blacklisted (banned sites: [\'T2_UK_SGrid_Bristol\', \'T2_US_MIT\'])', 'ASO destination is banned'
        assert sorted(resolver._expandSites(['T2_US*', 'T2_UK*'])) == sorted(['T2_US_Caltech', 'T2_US_Florida', 'T2_US_MIT', 'T2_US_Nebraska', 'T2_US_Purdue', 'T2_US_UCSD', 'T2_US_Vanderbilt', 'T2_US_Wisconsin', 'T2_UK_London_Brunel', 'T2_UK_London_IC', 'T2_UK_SGrid_Bristol', 'T2_UK_SGrid_RALPP']), 'Site expansion is incorrect' # pylint: disable=protected-access
    print('===== Test::All tests passed =====')
