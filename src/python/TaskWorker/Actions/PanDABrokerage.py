#pylint: skip-file
from __future__ import print_function
from PandaServerInterface import runBrokerage

from TaskWorker.Actions.PanDAAction import PanDAAction
from TaskWorker.DataObjects.Result import Result


class PanDABrokerage(PanDAAction):
    """Given a list of possible sites, ask PanDA which one is the
       best one at the current time for the job submission."""

    def execute(self, *args, **kwargs):
        results = []
        for jgroup in args[0]:
            possiblesites = jgroup.jobs[0]['input_files'][0]['locations']
            self.logger.debug("Possible sites == " + str(possiblesites))
            if len(possiblesites) == 0:
                msg = "DLS retourned no sites for the block"
                self.logger.error(msg)
                results.append(Result(task=kwargs['task'], result=(jgroup, None, []), err=msg))
                continue
            #use resubmiti white/black lists if we have them
            siteWhitelist = kwargs['task']['tm_site_whitelist'] if not kwargs['task']['resubmit_site_whitelist'] else kwargs['task']['resubmit_site_whitelist']
            siteBlacklist = kwargs['task']['tm_site_blacklist'] if not kwargs['task']['resubmit_site_blacklist'] else kwargs['task']['resubmit_site_blacklist']
            self.logger.debug("white list == %s" % set(siteWhitelist))
            self.logger.debug("black list == %s" % set(siteBlacklist))
            availablesites = list( set(possiblesites) & set(siteWhitelist) if siteWhitelist else set(possiblesites) - set(siteBlacklist) )
            self.logger.info('Available sites == %s' % str(availablesites))
            fixedsites = set(self.config.Sites.available)
            availablesites = list( set(availablesites) & fixedsites )
            if len(availablesites) == 0:
                msg = "No site available before brokering, will skip injection. Check White/Back lists"
                self.logger.error(msg)
                results.append(Result(task=kwargs['task'], result=(jgroup, None, []), err=msg))
                continue
            self.logger.info("Asking best site to PanDA between %s. Using %s as pandaserver." % (str(availablesites), self.backendurls['baseURLSSL']))
            selectedsite = runBrokerage(self.backendurls['baseURLSSL'], proxy=kwargs['task']['user_proxy'],
                                        sites=self.translateSiteName(availablesites))[-1]
            self.logger.info("Choosed site after brokering " +str(selectedsite))
            if not selectedsite:
                msg = "No site available after brokering, will skip injection"
                self.logger.error(msg)
                results.append(Result(task=kwargs['task'], result=(jgroup, None, []), err=msg))
                continue
            else:
                results.append(Result(task=kwargs['task'], result=(jgroup, selectedsite, availablesites)))
        return results

if __name__ == '__main__':
    ## here I show while I love Python's duck typing
    import collections
    task = {'tm_user_dn': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=mcinquil/CN=660800/CN=Mattia Cinquilli',
            'tm_user_vo': 'cms',
            'tm_user_group': '',
            'tm_user_role': '',
            'tm_site_blacklist': [],
            'tm_site_whitelist': [],}
    JobGroup = collections.namedtuple('JobGroup', 'jobs')
    jg = JobGroup(jobs=[{'input_files': [{'locations': ['T2_CH_CERN']}]}])
    Sites = collections.namedtuple('Sites', 'available')
    Config = collections.namedtuple('Config', 'Sites')
    sites = Sites(available=['T2_CH_CERN'])
    cfg = Config(Sites=sites)
    pb = PanDABrokerage(cfg)
    result = pb.execute([jg], task=task)
    for r in result:
        print(r)

    sites = Sites(available=[])
    cfg = Config(Sites=sites)
    pb = PanDABrokerage(cfg)
    result = pb.execute([jg], task=task)
    for r in result:
        print(r)
