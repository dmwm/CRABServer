"""
Resolve CRIC-related Sites info.
"""
# pylint: disable=too-many-branches

import json
from TaskWorker.DataObjects.Result import Result
from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.WorkerExceptions import SubmissionRefusedException

class SiteInfoResolver(TaskAction):
    """
    Given a task definition, resolve site white/black list with CRIC-related sites informations.
    """

    def __init__(self, config, crabserver, resourceCatalog=None, procnum=-1):
        """ need a comment line here """
        TaskAction.__init__(self, config, crabserver, procnum)
        self.resourceCatalog = resourceCatalog

    def execute(self, *args, **kwargs):
        global_blacklist = set(self.loadJSONFromFileInScratchDir('blacklistedSites.txt'))
        self.logger.debug("CRAB site blacklist: %s", list(global_blacklist))

        bannedOutDestinations = self.crabserver.get(api='info', data={'subresource': 'bannedoutdest'})[0]['result'][0]
        # self._checkASODestination(kwargs['task']['tm_asyncdest'], bannedOutDestinations)

        # siteWhitelist = kwargs['task']['tm_site_whitelist']
        # siteBlacklist = kwargs['task']['tm_site_blacklist']
        siteWhitelist = self._expandSites(set(kwargs['task']['tm_site_whitelist']))
        siteBlacklist = self._expandSites(set(kwargs['task']['tm_site_blacklist']))
        self.logger.debug("Site whitelist: %s", list(siteWhitelist))
        self.logger.debug("Site blacklist: %s", list(siteBlacklist))
        self.logger.debug("Site whitelist: %s", list(siteWhitelist))
        self.logger.debug("Site blacklist: %s", list(siteBlacklist))

        if siteWhitelist & global_blacklist:
            msg = f"The following sites from the user site whitelist are blacklisted by the CRAB server: {list(siteWhitelist & global_blacklist)}."
            msg += " Since the CRAB server blacklist has precedence, these sites are not considered in the user whitelist."
            self.uploadWarning(msg, kwargs['task']['user_proxy'], kwargs['task']['tm_taskname'])
            self.logger.warning(msg)

        if siteBlacklist & siteWhitelist:
            msg = f"The following sites appear in both the user site blacklist and whitelist: {list(siteBlacklist & siteWhitelist)}."
            msg += " Since the whitelist has precedence, these sites are not considered in the blacklist."
            self.uploadWarning(msg, kwargs['task']['user_proxy'], kwargs['task']['tm_taskname'])
            self.logger.warning(msg)

        ignoreLocality = kwargs['task']['tm_ignore_locality'] == 'T'
        self.logger.debug("Ignore locality: %s", ignoreLocality)

        if ignoreLocality:
            try:
                possiblesites = set(self.resourceCatalog.getAllPSNs()) - global_blacklist
            except Exception as ex:
                msg = "The CRAB3 server backend could not contact the Resource Catalog to get the list of all CMS sites."
                msg += " This could be a temporary Resource Catalog glitch."
                msg += " Please try to submit a new task (resubmit will not work)"
                msg += " and contact the experts if the error persists."
                msg += f"\nError reason: {ex}"
                raise TaskWorkerException(msg) from ex
        else:
            locations = set()
            possiblesites = locations
        ## At this point 'possiblesites' should never be empty.
        self.logger.debug("Possible sites: %s", list(possiblesites))
        self.logger.debug((kwargs['task'], possiblesites))

        return Result(task=kwargs['task'], result=(possiblesites, args[0]))

    def _expandSites(self, sites, pnn=False):
        """Check if there are sites cotaining the '*' wildcard and convert them in the corresponding list
           Raise exception if any wildcard site does expand to an empty list
           note that all*names.sites come from an HTTP query to CRIC which returns JSON and thus are unicode
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
                self._checkSite(site, pnn)
                res.add(site)
        return res

    def _checkASODestination(self, site, bannedOutDestinations=[]):
        """ Check that the ASO destination is correct and not among the banned ones
        """
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
    # > python3 DagmanCreator.py
    ###

    import logging
    from TaskWorker.WorkerUtilities import CRICService
    from WMCore.Configuration import loadConfigurationFile
    from ServerUtilities import newX509env
    config = loadConfigurationFile('/data/srv/TaskManager/cfg/TaskWorkerConfig.py')
    envForCMSWEB = newX509env(X509_USER_CERT=config.TaskWorker.cmscert, X509_USER_KEY=config.TaskWorker.cmskey)
    logging.basicConfig(level=logging.DEBUG)

    MOCK_BANNED_OUT_DESTINATIONS = ['T2_UK_SGrid_Bristol', 'T2_US_MIT']
    with envForCMSWEB:
        resourceCatalog = CRICService(logger=logging.getLogger(), configDict={"cacheduration": 1, "pycurl": True, "usestalecache": True})
        creator = DagmanCreator(config, crabserver=None, resourceCatalog=resourceCatalog)

        assert creator._checkSite('T2_US_Florida', pnn=False) is None, 'Site T2_US_Florida is valid'
        try:
            creator._checkSite('T2_TH_Bangkok', pnn=False)
        except ConfigException as ex:
            assert str(ex) == 'A site name T2_TH_Bangkok that user specified is not in the list of known CMS Processing Site Names', 'Site T2_TH_Bangkok are not existing and should be invalid'
        assert creator._checkASODestination('T2_US_Florida', MOCK_BANNED_OUT_DESTINATIONS) is None, 'Site: T2_US_Florida, Should not be banned'
        try:
            creator._checkASODestination('T2_UK_SGrid_Bristol', MOCK_BANNED_OUT_DESTINATIONS)
        except ConfigException as ex:
            assert str(ex) == 'Remote output data site is banned, The output site you specified in the Site.storageSite parameter (T2_UK_SGrid_Bristol) is blacklisted (banned sites: [\'T2_UK_SGrid_Bristol\', \'T2_US_MIT\'])', 'ASO destination is banned'
        assert sorted(creator._expandSites(['T2_US*', 'T2_UK*'])) == sorted(['T2_US_Caltech', 'T2_US_Florida', 'T2_US_MIT', 'T2_US_Nebraska', 'T2_US_Purdue', 'T2_US_UCSD', 'T2_US_Vanderbilt', 'T2_US_Wisconsin', 'T2_UK_London_Brunel', 'T2_UK_London_IC', 'T2_UK_SGrid_Bristol', 'T2_UK_SGrid_RALPP']), 'Site expansion is incorrect'
    print('===== Test::All tests passed =====')
