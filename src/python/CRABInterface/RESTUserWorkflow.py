# WMCore dependecies here
from WMCore.REST.Error import ExecutionError, InvalidParameter
from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Validation import validate_str, validate_strlist, validate_num, validate_numlist
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import allScramArchsAndVersions, TAG_COLLECTOR_URL
from WMCore.Lexicon import userprocdataset, userProcDSParts, primdataset

# CRABServer dependecies here
from CRABInterface.DataUserWorkflow import DataUserWorkflow
from CRABInterface.DataUserWorkflow import DataWorkflow
from CRABInterface.RESTExtensions import authz_owner_match, authz_login_valid
from CRABInterface.Regexps import *
from CRABInterface.Utils import CMSSitesCache, conn_handler, getDBinstance
from ServerUtilities import checkOutLFN

# external dependecies here
import cherrypy
import time
import logging
import re


class RESTUserWorkflow(RESTEntity):
    """REST entity for workflows from the user point of view and relative subresources"""

    def __init__(self, app, api, config, mount, centralcfg):
        RESTEntity.__init__(self, app, api, config, mount)

        self.logger = logging.getLogger("CRABLogger.RESTUserWorkflow")
        self.userworkflowmgr = DataUserWorkflow()
        self.allCMSNames = CMSSitesCache(cachetime=0, sites={})
        self.centralcfg = centralcfg
        self.Task = getDBinstance(config, 'TaskDB', 'Task')

    def _expandSites(self, sites):
        """Check if there are sites cotaining the '*' wildcard and convert them in the corresponding list
           Raise exception if any wildcard site does expand to an empty list
        """
        res = set()
        for site in sites:
            if '*' in site:
                sitere = re.compile(site.replace('*','.*'))
                expanded = map(str, filter(sitere.match, self.allCMSNames.sites))
                self.logger.debug("Site %s expanded to %s during validate" % (site, expanded))
                if not expanded:
                    excasync = ValueError("Remote output data site not valid")
                    invalidp = InvalidParameter("Cannot expand site %s to anything" % site, errobj = excasync)
                    setattr(invalidp, 'trace', '')
                    raise invalidp
                res = res.union(expanded)
            else:
                self._checkSite(site)
                res.add(site)
        return list(res)

    def _checkOutLFN(self, kwargs):
        """Check the lfn parameter: it must start with '/store/user/<username>/', '/store/group/groupname/' or '/store/local/something/',
           where username is the one registered in SiteDB (i.e. the one used in the CERN primary account).
           If lfn is not there, default to '/store/user/<username>/'.
        """
        ## This is the username registered in SiteDB.
        username = cherrypy.request.user['login']
        if not kwargs['lfn']:
            ## Default to '/store/user/<username>/' if the user did not specify the lfn parameter.
            kwargs['lfn'] = '/store/user/%s/' % (username)
        else:
            msg  = "The parameter Data.outLFNDirBase in the CRAB configuration file must start with either"
            msg += " '/store/user/<username>/' or '/store/group/<groupname>/'"
            msg += " (or '/store/local/<something>/' if publication is off),"
            msg += " where username is your username as registered in SiteDB"
            msg += " (i.e. the username of your CERN primary account)."
            if not checkOutLFN(kwargs['lfn'], username):
                raise InvalidParameter(msg)

    def _checkPublishDataName(self, kwargs):
        """
        Validate the (user specified part of the) output dataset name for publication
        using the WMCore.Lexicon method userprocdataset(), which does the same
        validation as DBS (see discussion with Yuyi Guo in following github CRABClient
        issue: https://github.com/dmwm/CRABClient/issues/4257).
        """
        ## These if statements should actually never evaluate to True, because
        ## the client always defines kwargs['publishname'] and kwargs['workflow'].
        if 'publishname' not in kwargs:
            msg  = "Server parameter 'publishname' is not defined."
            msg += " Unable to validate the publication dataset name."
            raise InvalidParameter(msg)
        if kwargs['publishname'].find('-') == -1 and 'workflow' not in kwargs:
            msg  = "Server parameter 'workflow' is not defined."
            msg += " Unable to validate the publication dataset name."
            raise InvalidParameter(msg)
        ## The client defines kwargs['publishname'] = <Data.publishDataName>-<isbchecksum>
        ## if the user defines Data.publishDataName, and kwargs['publishname'] = <isbchecksum> otherwise.
        ## (The PostJob replaces then the isbchecksum by the psethash.)
        ## In DataWorkflow.py, the publish_name column of the TaskDB is defined
        ## in this way (without "username-"), and that's what is used later by
        ## PostJob to define the output dataset name.
        if kwargs['publishname'].find('-') == -1:
            publishDataNameToCheck = "username-%s-%s" % (kwargs['workflow'].replace(':','_'), kwargs['publishname'])
        else:
            publishDataNameToCheck = "username-%s" % (kwargs['publishname'])
        try:
            userprocdataset(publishDataNameToCheck)
        except AssertionError:
            ## The messages below are more descriptive than if we would use
            ## the message from AssertionError exception.
            if kwargs['publishname'].find('-') == -1:
                msg  = "Invalid CRAB configuration parameter General.requestName."
                msg += " The parameter should not have more than 152 characters"
                msg += " and should match the regular expression %s" % (userProcDSParts['publishdataname'])
            else:
                msg  = "Invalid CRAB configuration parameter Data.publishDataName."
                msg += " The parameter should not have more than 157 characters"
                msg += " and should match the regular expression %s" % (userProcDSParts['publishdataname'])
            raise InvalidParameter(msg)

    def _checkPrimaryDataset(self, kwargs):
        """
        Validate the primary dataset name using the WMCore.Lexicon method primdataset(),
        which does the same validation as DBS (see discussion with Yuyi Guo in following
        github CRABClient issue: https://github.com/dmwm/CRABClient/issues/4257).
        """
        ## This if statement should actually never evaluate to True, because
        ## the client always defines kwargs['inputdata'].
        if 'inputdata' not in kwargs:
            msg  = "Server parameter 'inputdata' is not defined."
            msg += " Unable to validate the primary dataset name."
            raise InvalidParameter(msg)
        try:
            ## The client defines kwargs['inputdata'] = "/<primary-dataset-name>",
            ## i.e. it adds a "/" at the beginning, which we should remove if we
            ## want to do the validation of the primary dataset name only.
            ## (The client puts the primary dataset name in kwargs['inputdata'],
            ## because the PostJob extracts the primary dataset name from this
            ## parameter splitting by "/" and taking the element 1, which is the
            ## right thing to do when kwargs['inputdata'] is the input dataset in
            ## an analysis job type.)
            primdataset(kwargs['inputdata'][1:])
        except AssertionError:
            ## This message is more descriptive than if we would use the message
            ## from AssertionError exception, but I had to explicitely write the
            ## regular expression [a-zA-Z][a-zA-Z0-9\-_]*, which is not nice.
            ## The message from AssertionError exception would be:
            ## "'<kwargs['inputdata'][1:]>' does not match regular expression [a-zA-Z0-9\.\-_]+".
            msg  = "Invalid CRAB configuration parameter Data.primaryDataset."
            msg += " The parameter should not have more than 99 characters"
            msg += " and should match the regular expression [a-zA-Z][a-zA-Z0-9\-_]*"
            raise InvalidParameter(msg)

    def _checkASODestination(self, site):
        self._checkSite(site)
        if site in self.centralcfg.centralconfig.get('banned-out-destinations', []):
            excasync = ValueError("Remote output data site is banned")
            invalidp = InvalidParameter("The output site you specified in the Site.storageSite parameter (%s) is blacklisted (banned sites: %s)" %\
                            (site, self.centralcfg.centralconfig['banned-out-destinations']), errobj = excasync)
            setattr(invalidp, 'trace', '')
            raise invalidp

    def _checkSite(self, site):
        if site not in self.allCMSNames.sites:
            excasync = ValueError("A site name you specified is not valid")
            invalidp = InvalidParameter("The parameter %s is not in the list of known CMS sites %s" % (site, self.allCMSNames.sites), errobj = excasync)
            setattr(invalidp, 'trace', '')
            raise invalidp

    def _checkReleases(self, jobarch, jobsw):
        """ Check if the software needed by the user is available in the tag collector
            Uses allScramArchsAndVersions from WMCore. If an IOError is raised report an error message.
            If the list of releases is empty (reason may be an ExpatError) then report an error message
            If the asked released is not there then report an error message
        """
        msg = False
        goodReleases = {}
        try:
            goodReleases = allScramArchsAndVersions()
        except IOError:
            msg = "Error connecting to %s and determine the list of available releases. " % TAG_COLLECTOR_URL +\
                  "Skipping the check of the releases"
        else:
            if goodReleases == {}:
                msg = "The list of releases at %s is empty. " % TAG_COLLECTOR_URL +\
                      "Skipping the check of the releases"
            elif jobarch not in goodReleases or jobsw not in goodReleases[jobarch]:
                msg = "ERROR: %s on %s is not among supported releases" % (jobsw, jobarch)
                msg += "\nUse config.JobType.allowUndistributedCMSSW = True if you are sure of what you are doing"
                excasync = "ERROR: %s on %s is not among supported releases or an error occurred" % (jobsw, jobarch)
                invalidp = InvalidParameter(msg, errobj = excasync)
                setattr(invalidp, 'trace', '')
                raise invalidp

        if msg:
            #Need to log the message in the db for the users
            self.logger.warning(msg)

    @conn_handler(services=['sitedb', 'centralconfig'])
    def validate(self, apiobj, method, api, param, safe):
        """Validating all the input parameter as enforced by the WMCore.REST module"""
        #authz_login_valid()

        if method in ['PUT']:
            validate_str("activity", param, safe, RX_ACTIVITY, optional=True)
            validate_str("jobtype", param, safe, RX_JOBTYPE, optional=False)
            # TODO this should be changed to be non-optional
            validate_str("generator", param, safe, RX_GENERATOR, optional=True)
            validate_str("eventsperlumi", param, safe, RX_LUMIEVENTS, optional=True)
            validate_str("jobsw", param, safe, RX_CMSSW, optional=False)
            validate_num("nonprodsw", param, safe, optional=False)
            validate_str("jobarch", param, safe, RX_ARCH, optional=False)
            if not safe.kwargs["nonprodsw"]: #if the user wants to allow non-production releases
                self._checkReleases(safe.kwargs['jobarch'], safe.kwargs['jobsw'])
            jobtype = safe.kwargs.get('jobtype', None)
            validate_num("useparent", param, safe, optional=True)
            validate_strlist("siteblacklist", param, safe, RX_CMSSITE)
            safe.kwargs['siteblacklist'] = self._expandSites(safe.kwargs['siteblacklist'])
            validate_strlist("sitewhitelist", param, safe, RX_CMSSITE)
            safe.kwargs['sitewhitelist'] = self._expandSites(safe.kwargs['sitewhitelist'])
            validate_str("splitalgo", param, safe, RX_SPLIT, optional=False)
            validate_num("algoargs", param, safe, optional=False)
            validate_num("totalunits", param, safe, optional=True)
            validate_str("cachefilename", param, safe, RX_CACHENAME, optional=False)
            validate_str("cacheurl", param, safe, RX_CACHEURL, optional=False)
            validate_str("lfn", param, safe, RX_LFN, optional=True)
            self._checkOutLFN(safe.kwargs)
            validate_strlist("addoutputfiles", param, safe, RX_ADDFILE)
            validate_strlist("userfiles", param, safe, RX_USERFILE)
            validate_num("savelogsflag", param, safe, optional=False)
            validate_num("saveoutput", param, safe, optional=True)
            validate_num("faillimit", param, safe, optional=True)
            validate_num("ignorelocality", param, safe, optional=True)
            if safe.kwargs['ignorelocality'] and self.centralcfg.centralconfig.get('ign-locality-blacklist', []):
                safe.kwargs['siteblacklist'] += self._expandSites(self.centralcfg.centralconfig['ign-locality-blacklist'])
            validate_str("vorole", param, safe, RX_VOPARAMS, optional=True)
            validate_str("vogroup", param, safe, RX_VOPARAMS, optional=True)
            validate_num("publication", param, safe, optional=False)
            validate_str("publishdbsurl", param, safe, RX_DBSURL, optional=(not bool(safe.kwargs['publication'])))
            ## Validations needed in case publication is on.
            if safe.kwargs['publication']:
                ## The (user specified part of the) publication dataset name must be
                ## specified and must pass DBS validation. Since this is the correct
                ## validation function, it must be done before the
                ## validate_str("workflow", ...) and validate_str("publishname", ...)
                ## we have below. Not sure why RX_PUBLISH was introduced in CRAB, but
                ## it is not the correct regular expression for publication dataset
                ## name validation.
                self._checkPublishDataName(param.kwargs)
                ## 'publishname' was already validated above in _checkPublishDataName().
                ## But I am not sure if we can skip the validate_str('publishname', ...)
                ## or we need it so that all parameters are moved from param to safe.
                ## Therefore, I didn't remove the validate_str('publishname', ...),
                ## but only changed RX_PUBLISH -> RX_ANYTHING.
                validate_str('publishname', param, safe, RX_ANYTHING, optional=False)
                ## If this is a MC generation job type, the primary dataset name will
                ## be used to define the primary dataset field in the publication
                ## dataset name. Therefore, the primary dataset name must pass DBS
                ## validation. Since in a MC generation job type the primary dataset
                ## name is saved in 'inputdata', _checkPrimaryDataset() actually
                ## validates 'inputdata'. Therefore, this function must come before
                ## the validate_str('inputdata', ...) we have below. OTOH, matching
                ## 'inputdata' agains RX_DATASET is not the correct validation for
                ## the primary dataset name.
                if jobtype == 'PrivateMC':
                    self._checkPrimaryDataset(param.kwargs)
            else:
                ## Not sure if we need to match publishname against RX_PUBLISH...
                validate_str("publishname", param, safe, RX_PUBLISH, optional=False)
            ## This line must come after _checkPublishDataName()
            validate_str("workflow", param, safe, RX_WORKFLOW, optional=False)
            if jobtype == 'Analysis':
                validate_str("inputdata", param, safe, RX_DATASET, optional=False)
            ## Added this if, because in this case 'inputdata' was already validated
            ## above in _checkPrimaryDataset(), and I am not sure if we can skip the
            ## validate_str('inputdata', ...) or we need it so that all parameters
            ## are moved from param to safe.
            elif jobtype == 'PrivateMC' and safe.kwargs['publication']:
                validate_str("inputdata", param, safe, RX_ANYTHING, optional=False)
            else:
                validate_str("inputdata", param, safe, RX_DATASET, optional=True)
            #if one and only one between publishDataName and publishDbsUrl is set raise an error (we need both or none of them)
            validate_str("asyncdest", param, safe, RX_CMSSITE, optional=False)
            self._checkASODestination(safe.kwargs['asyncdest'])
            # We no longer use this attribute, but keep it around for older client compatibility
            validate_num("blacklistT1", param, safe, optional=True)
            validate_num("oneEventMode", param, safe, optional=True)
            validate_num("priority", param, safe, optional=True)
            validate_num("maxjobruntime", param, safe, optional=True)
            validate_num("numcores", param, safe, optional=True)
            validate_num("maxmemory", param, safe, optional=True)
            validate_str("dbsurl", param, safe, RX_DBSURL, optional=False)
            validate_strlist("tfileoutfiles", param, safe, RX_OUTFILES)
            validate_strlist("edmoutfiles", param, safe, RX_OUTFILES)
            validate_strlist("runs", param, safe, RX_RUNS)
            validate_strlist("lumis", param, safe, RX_LUMIRANGE)
            #validate_str("scheduler", param, safe, RX_SCHEDULER)
            if len(safe.kwargs["runs"]) != len(safe.kwargs["lumis"]):
                raise InvalidParameter("The number of runs and the number of lumis lists are different")
            validate_strlist("adduserfiles", param, safe, RX_ADDFILE)
            validate_str("asourl", param, safe, RX_ASOURL, optional=True)
            validate_str("scriptexe", param, safe, RX_ADDFILE, optional=True)
            validate_strlist("scriptargs", param, safe, RX_SCRIPTARGS)
            validate_str("scheddname", param, safe, RX_SCHEDD_NAME, optional=True)
            validate_str("collector", param, safe, RX_COLLECTOR, optional=True)
            validate_strlist("extrajdl", param, safe, RX_SCRIPTARGS)
            validate_num("dryrun", param, safe, optional=True)

        elif method in ['POST']:
            validate_str("workflow", param, safe, RX_UNIQUEWF, optional=False)
            validate_str("subresource", param, safe, RX_SUBRESTAT, optional=True)
            ## In a resubmission, the site black- and whitelists need to be interpreted
            ## differently than in an initial task submission. If there is no site black-
            ## or whitelist, set it to None and DataWorkflow will use the corresponding
            ## list defined in the initial task submission. If the site black- or whitelist
            ## is equal to the string 'empty', set it to an empty list and don't call
            ## validate_strlist as it would fail.
            if 'siteblacklist' not in param.kwargs:
                safe.kwargs['siteblacklist'] = None
            elif param.kwargs['siteblacklist'] == 'empty':
                safe.kwargs['siteblacklist'] = []
                del param.kwargs['siteblacklist']
            else:
                validate_strlist("siteblacklist", param, safe, RX_CMSSITE)
                safe.kwargs['siteblacklist'] = self._expandSites(safe.kwargs['siteblacklist'])
            if 'sitewhitelist' not in param.kwargs:
                safe.kwargs['sitewhitelist'] = None
            elif param.kwargs['sitewhitelist'] == 'empty':
                safe.kwargs['sitewhitelist'] = []
                del param.kwargs['sitewhitelist']
            else:
                validate_strlist("sitewhitelist", param, safe, RX_CMSSITE)
                safe.kwargs['sitewhitelist'] = self._expandSites(safe.kwargs['sitewhitelist'])
            validate_numlist('jobids', param, safe)
            validate_num("priority", param, safe, optional=True)
            validate_num("maxjobruntime", param, safe, optional=True)
            validate_num("numcores", param, safe, optional=True)
            validate_num("maxmemory", param, safe, optional=True)
            validate_num("force", param, safe, optional=True)

        elif method in ['GET']:
            validate_str("workflow", param, safe, RX_UNIQUEWF, optional=True)
            validate_str('subresource', param, safe, RX_SUBRESTAT, optional=True)
            validate_str('username', param, safe, RX_USERNAME, optional=True)
            validate_str('timestamp', param, safe, RX_DATE, optional=True) ## inserted by eric

            ## Used to determine how much information to return to the client for status.
            ## also used by report to determine if it has to check job states
            validate_num("verbose", param, safe, optional=True)

            ## used by get log, get data
            validate_num('limit', param, safe, optional=True)
            validate_num('exitcode', param, safe, optional=True)
            validate_numlist('jobids', param, safe)

            ## used by errors and report (short format in report means we do not query DBS)
            validate_num('shortformat', param, safe, optional=True)

            ## validation parameters
            if not safe.kwargs['workflow'] and safe.kwargs['subresource']:
                raise InvalidParameter("Invalid input parameters")
            if safe.kwargs['subresource'] in ['data', 'logs'] and not safe.kwargs['limit'] and not safe.kwargs['jobids']:
                raise InvalidParameter("You need to specify the number of jobs to retrieve or their ids.")

        elif method in ['DELETE']:
            validate_str("workflow", param, safe, RX_UNIQUEWF, optional=False)
            validate_num("force", param, safe, optional=True)
            validate_numlist('jobids', param, safe)


    @restcall
    #@getUserCert(headers=cherrypy.request.headers)
    def put(self, workflow, activity, jobtype, jobsw, jobarch, inputdata, useparent, generator, eventsperlumi, siteblacklist, sitewhitelist, splitalgo, algoargs, cachefilename, cacheurl, addoutputfiles,\
                savelogsflag, publication, publishname, asyncdest, dbsurl, publishdbsurl, vorole, vogroup, tfileoutfiles, edmoutfiles, runs, lumis,\
                totalunits, adduserfiles, oneEventMode, maxjobruntime, numcores, maxmemory, priority, blacklistT1, nonprodsw, lfn, saveoutput,
                faillimit, ignorelocality, userfiles, asourl, scriptexe, scriptargs, scheddname, extrajdl, collector, dryrun):
        """Perform the workflow injection

           :arg str workflow: workflow name requested by the user;
           :arg str activity: workflow activity type, default None;
           :arg str jobtype: job type of the workflow, usually Analysis;
           :arg str jobsw: software requirement;
           :arg str jobarch: software architecture (=SCRAM_ARCH);
           :arg str inputdata: input dataset;
           :arg int useparent: add the parent dataset as secondary input;
           :arg str generator: event generator for MC production;
           :arg str eventsperlumi: how many events to generate per lumi;
           :arg str list siteblacklist: black list of sites, with CMS name;
           :arg str list sitewhitelist: white list of sites, with CMS name;
           :arg str splitalgo: algorithm to be used for the workflow splitting;
           :arg str algoargs: argument to be used by the splitting algorithm;
           :arg str cachefilename: name of the file inside the cache
           :arg str cacheurl: URL of the cache
           :arg str list addoutputfiles: list of additional output files;
           :arg int savelogsflag: archive the log files? 0 no, everything else yes;
           :arg str userdn: DN of user doing the request;
           :arg str userhn: hyper new name of the user doing the request;
           :arg int publication: flag enabling or disabling data publication;
           :arg str publishname: name to use for data publication;
           :arg str asyncdest: CMS site name for storage destination of the output files;
           :arg str dbsurl: dbs url where the input dataset is published;
           :arg str publishdbsurl: dbs url where the output data has to be published;
           :arg str vorole: user vo role
           :arg str vogroup: user vo group
           :arg str tfileoutfiles: list of t-output files
           :arg str edmoutfiles: list of edm output files
           :arg str list runs: list of run numbers
           :arg str list lumis: list of lumi section numbers
           :arg str scheduler: Which scheduler to use, can be 'panda' or 'condor'
           :arg int totalunits: number of MC event to be generated
           :arg int adduserfiles: additional user file to be copied in the cmsRun directory
           :arg int oneEventMode: flag enabling oneEventMode
           :arg int maxjobruntime: max job runtime, in minutes
           :arg int numcores: number of CPU cores required by job
           :arg int maxmemory: maximum amount of RAM required, in MB
           :arg int priority: priority of this task
           :arg int saveoutput: whether to perform stageout for a given output file
           :arg int faillimit: the maximum number of failed jobs which triggers a workflow abort.
           :arg int ignorelocality: whether to ignore file locality in favor of the whitelist.
           :arg str userfiles: The files to process instead of a DBS-based dataset.
           :arg str asourl: ASO url to be used in place of the one in the ext configuration.
           :arg str scriptexe: script to execute in place of cmsrun.
           :arg str scriptargs: arguments to be passed to the scriptexe script.
           :arg str scheddname: Schedd Name used for debugging.
           :arg str extrajdl: extra Job Description Language parameters to be added.
           :arg str collector: Collector Name used for debugging.
           :arg int dryrun: enable dry run mode (initialize but do not submit request).
           :returns: a dict which contaians details of the request"""

        #print 'cherrypy headers: %s' % cherrypy.request.headers['Ssl-Client-Cert']
        return self.userworkflowmgr.submit(workflow=workflow, activity=activity, jobtype=jobtype, jobsw=jobsw, jobarch=jobarch,
                                       inputdata=inputdata, use_parent=useparent, generator=generator, events_per_lumi=eventsperlumi,
                                       siteblacklist=siteblacklist, sitewhitelist=sitewhitelist, splitalgo=splitalgo, algoargs=algoargs,
                                       cachefilename=cachefilename, cacheurl=cacheurl,
                                       addoutputfiles=addoutputfiles, userdn=cherrypy.request.user['dn'],
                                       userhn=cherrypy.request.user['login'], savelogsflag=savelogsflag, vorole=vorole, vogroup=vogroup,
                                       publication=publication, publishname=publishname, asyncdest=asyncdest,
                                       dbsurl=dbsurl, publishdbsurl=publishdbsurl, tfileoutfiles=tfileoutfiles,
                                       edmoutfiles=edmoutfiles, runs=runs, lumis=lumis, totalunits=totalunits, adduserfiles=adduserfiles, oneEventMode=oneEventMode,
                                       maxjobruntime=maxjobruntime, numcores=numcores, maxmemory=maxmemory, priority=priority, lfn=lfn,
                                       ignorelocality=ignorelocality, saveoutput=saveoutput, faillimit=faillimit, userfiles=userfiles, asourl=asourl,
                                       scriptexe=scriptexe, scriptargs=scriptargs, scheddname=scheddname, extrajdl=extrajdl, collector=collector, dryrun=dryrun)

    @restcall
    def post(self, workflow, subresource, siteblacklist, sitewhitelist, jobids, maxjobruntime, numcores, maxmemory, priority, force=0): # default value for force is only for backward compatibility.
        """Resubmit or continue an existing workflow. The caller needs to be a CMS user owner of the workflow.

           :arg str workflow: unique name identifier of the workflow;
           :arg str list siteblacklist: black list of sites, with CMS name;
           :arg str list sitewhitelist: white list of sites, with CMS name."""
        # strict check on authz: only the workflow owner can modify it
        authz_owner_match(self.api, [workflow], self.Task)
        if not subresource or subresource == 'resubmit':
            return self.userworkflowmgr.resubmit(workflow=workflow, \
                                                 siteblacklist=siteblacklist, sitewhitelist=sitewhitelist, jobids=jobids, \
                                                 maxjobruntime=maxjobruntime, numcores=numcores, maxmemory=maxmemory, priority=priority, force=force, \
                                                 userdn=cherrypy.request.headers['Cms-Authn-Dn'])
        elif subresource == 'proceed':
            return self.userworkflowmgr.proceed(workflow=workflow)

    @restcall
    def get(self, workflow, subresource, username, limit, shortformat, exitcode, jobids, verbose, timestamp):
        """Retrieves the workflow information, like a status summary, in case the workflow unique name is specified.
           Otherwise returns all workflows since (now - age) for which the user is the owner.
           The caller needs to be a valid CMS user.
           :arg str workflow: unique name identifier of workflow;
		   :arg str timestamp: max workflow age in hours;
           :arg str subresource: the specific workflow information to be accessed;
           :arg int limit: limit of return entries for some specific subresource;
           :arg int exitcode: exitcode for which the specific subresource is needed (eg log file of a job with that exitcode)
           :retrun: workflow with the relative status summary in case of per user request; or
                    the requested subresource."""
        result = []
        if workflow:
            userdn=cherrypy.request.headers['Cms-Authn-Dn']
            # if have the wf then retrieve the wf status summary
            if not subresource:
                result = self.userworkflowmgr.status(workflow, verbose=verbose, userdn=userdn)
            # if have a subresource then it should be one of these
            elif subresource == 'logs':
                result = self.userworkflowmgr.logs(workflow, limit, exitcode, jobids, userdn=userdn)
            elif subresource == 'data':
                result = self.userworkflowmgr.output(workflow, limit, jobids, userdn=userdn)
            elif subresource == 'errors':
                result = self.userworkflowmgr.errors(workflow, shortformat)
            elif subresource == 'report':
                result = self.userworkflowmgr.report(workflow, userdn=userdn, usedbs=shortformat)
            # if here means that no valid subresource has been requested
            # flow should never pass through here since validation restrict this
            else:
                raise ExecutionError("Validation or method error")
        else:
            # retrieve the information about latest worfklows for that user
            # age can have a default: 1 week ?
            cherrypy.log("Found user '%s'" % cherrypy.request.user['login'])
            result = self.userworkflowmgr.getLatests(username or cherrypy.request.user['login'], timestamp)     #eric added timestamp to match username

        return result

    @restcall
    def delete(self, workflow, force, jobids):
        """Aborts a workflow. The user needs to be a CMS owner of the workflow.

           :arg str list workflow: list of unique name identifiers of workflows;
           :arg int force: force to delete the workflows in any case; 0 no, everything else yes;
           :return: nothing"""

        # strict check on authz: only the workflow owner can modify it
        authz_owner_match(self.api, [workflow], self.Task)
        return self.userworkflowmgr.kill(workflow, force, jobids, userdn=cherrypy.request.headers['Cms-Authn-Dn'])
