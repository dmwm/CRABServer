# WMCore dep#endecies here
from WMCore.REST.Error import ExecutionError, InvalidParameter
from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Validation import validate_str, validate_strlist, validate_num, validate_numlist

# CRABServer dependecies here
from CRABInterface.DataUserWorkflow import DataUserWorkflow
from CRABInterface.DataUserWorkflow import DataWorkflow
from CRABInterface.RESTExtensions import authz_owner_match, authz_login_valid
from CRABInterface.Regexps import *
from CRABInterface.Utils import CMSSitesCache, conn_handler

# external dependecies here
import cherrypy
import time
import logging
import re


class RESTUserWorkflow(RESTEntity):
    """REST entity for workflows from the user point of view and relative subresources"""

    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)

        self.logger = logging.getLogger("CRABLogger.RESTUserWorkflow")
        self.userworkflowmgr = DataUserWorkflow()
        self.allCMSNames = CMSSitesCache(cachetime=0, sites={})

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

    def _checkSite(self, site):
        if site not in self.allCMSNames.sites:
            excasync = ValueError("Remote output data site not valid")
            invalidp = InvalidParameter("The parameter %s is not in the list of known CMS sites %s" % (site, self.allCMSNames.sites), errobj = excasync)
            setattr(invalidp, 'trace', '')
            raise invalidp

    @conn_handler(services=['sitedb'])
    def validate(self, apiobj, method, api, param, safe):
        """Validating all the input parameter as enforced by the WMCore.REST module"""
        authz_login_valid()

        if method in ['PUT']:
            validate_str("workflow", param, safe, RX_WORKFLOW, optional=False)
            validate_str("jobtype", param, safe, RX_JOBTYPE, optional=False)
            validate_str("jobsw", param, safe, RX_CMSSW, optional=False)
            validate_str("jobarch", param, safe, RX_ARCH, optional=False)
            jobtype = safe.kwargs.get('jobtype', None)
            if jobtype == 'Analysis':
                validate_str("inputdata", param, safe, RX_DATASET, optional=False)
            else:
                validate_str("inputdata", param, safe, RX_DATASET, optional=True)
            validate_strlist("siteblacklist", param, safe, RX_CMSSITE)
            safe.kwargs['siteblacklist'] = self._expandSites(safe.kwargs['siteblacklist'])
            validate_strlist("sitewhitelist", param, safe, RX_CMSSITE)
            safe.kwargs['sitewhitelist'] = self._expandSites(safe.kwargs['sitewhitelist'])
            validate_str("splitalgo", param, safe, RX_SPLIT, optional=False)
            validate_num("algoargs", param, safe, optional=False)
            validate_num("totalunits", param, safe, optional=True)
            validate_str("cachefilename", param, safe, RX_CACHENAME, optional=False)
            validate_str("cacheurl", param, safe, RX_CACHEURL, optional=False)
            #validate_str("userisburl", param, safe, re.compile(r"^[A-Za-z]*$"), optional=False)
            validate_strlist("addoutputfiles", param, safe, RX_ADDFILE)
            validate_num("savelogsflag", param, safe, optional=False)
            validate_str("vorole", param, safe, RX_VOPARAMS, optional=True)
            validate_str("vogroup", param, safe, RX_VOPARAMS, optional=True)
            validate_str("publishname", param, safe, RX_PUBLISH, optional=False)
            validate_str("publishdbsurl", param, safe, RX_DBSURL, optional=True)
            validate_num("publication", param, safe, optional=False)
            #if publication is set as true both publishDataName and publishDbsUrl are needed
            if safe.kwargs["publication"] and not (bool(safe.kwargs["publishname"]) and bool(safe.kwargs["publishdbsurl"])):
                raise InvalidParameter("You need to set both publishDataName and publishDbsUrl parameters if you need the automatic publication")
            #if one and only one between publishDataName and publishDbsUrl is set raise an error (we need both or none of them)
            validate_str("asyncdest", param, safe, RX_CMSSITE, optional=False)
            self._checkSite(safe.kwargs['asyncdest'])
            validate_num("blacklistT1", param, safe, optional=False)
            validate_str("dbsurl", param, safe, RX_DBSURL, optional=False)
            validate_strlist("tfileoutfiles", param, safe, RX_OUTFILES)
            validate_strlist("edmoutfiles", param, safe, RX_OUTFILES)
            validate_strlist("runs", param, safe, RX_RUNS)
            validate_strlist("lumis", param, safe, RX_LUMIRANGE)
            if len(safe.kwargs["runs"]) != len(safe.kwargs["lumis"]):
                raise InvalidParameter("The number of runs and the number of lumis lists are different")
            validate_strlist("adduserfiles", param, safe, RX_ADDFILE)

        elif method in ['POST']:
            validate_str("workflow", param, safe, RX_UNIQUEWF, optional=False)
            validate_strlist("siteblacklist", param, safe, RX_CMSSITE)
            safe.kwargs['siteblacklist'] = self._expandSites(safe.kwargs['siteblacklist'])
            validate_strlist("sitewhitelist", param, safe, RX_CMSSITE)
            safe.kwargs['sitewhitelist'] = self._expandSites(safe.kwargs['sitewhitelist'])
            validate_numlist('jobids', param, safe)

        elif method in ['GET']:
            validate_str("workflow", param, safe, RX_UNIQUEWF, optional=True)
            validate_str('subresource', param, safe, RX_SUBRESTAT, optional=True)

            #parameters of subresources calls has to be put here
            #used by get latest
            validate_num('age', param, safe, optional=True)

            #used by get log, get data
            validate_num('limit', param, safe, optional=True)
            validate_num('exitcode', param, safe, optional=True)
            validate_numlist('jobids', param, safe)

            #used by errors
            validate_num('shortformat', param, safe, optional=True)

            #validation parameters
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
    def put(self, workflow, jobtype, jobsw, jobarch, inputdata, siteblacklist, sitewhitelist, splitalgo, algoargs, cachefilename, cacheurl, addoutputfiles,\
               savelogsflag, publication, publishname, asyncdest, blacklistT1, dbsurl, publishdbsurl, vorole, vogroup, tfileoutfiles, edmoutfiles, runs, lumis,\
                totalunits, adduserfiles):
        """Perform the workflow injection

           :arg str workflow: workflow name requested by the user;
           :arg str jobtype: job type of the workflow, usually Analysis;
           :arg str jobsw: software requirement;
           :arg str jobarch: software architecture (=SCRAM_ARCH);
           :arg str inputdata: input dataset;
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
           :arg int blacklistT1: flag enabling or disabling the black listing of Tier-1 sites;
           :arg str dbsurl: dbs url where the input dataset is published;
           :arg str publishdbsurl: dbs url where the output data has to be published;
           :arg str vorole: user vo role
           :arg str vogroup: user vo group
           :arg str tfileoutfiles: list of t-output files
           :arg str edmoutfiles: list of edm output files
           :arg str list runs: list of run numbers
           :arg str list lumis: list of lumi section numbers
           :arg int totalunits: number of MC event to be generated
           :arg int adduserfiles: additional user file to be copied in the cmsRun directory
           :returns: a dict which contaians details of the request"""

        #print 'cherrypy headers: %s' % cherrypy.request.headers['Ssl-Client-Cert']
        return self.userworkflowmgr.submit(workflow=workflow, jobtype=jobtype, jobsw=jobsw, jobarch=jobarch, inputdata=inputdata,
                                       siteblacklist=siteblacklist, sitewhitelist=sitewhitelist, splitalgo=splitalgo, algoargs=algoargs,
                                       cachefilename=cachefilename, cacheurl=cacheurl,
                                       addoutputfiles=addoutputfiles, userdn=cherrypy.request.user['dn'],
                                       userhn=cherrypy.request.user['login'], savelogsflag=savelogsflag, vorole=vorole, vogroup=vogroup,
                                       publication=publication, publishname=publishname, asyncdest=asyncdest, blacklistT1=blacklistT1,
                                       dbsurl=dbsurl, publishdbsurl=publishdbsurl, tfileoutfiles=tfileoutfiles,\
                                       edmoutfiles=edmoutfiles, runs=runs, lumis=lumis, totalunits=totalunits, adduserfiles=adduserfiles)

    @restcall
    def post(self, workflow, siteblacklist, sitewhitelist, jobids):
        """Resubmit an existing workflow. The caller needs to be a CMS user owner of the workflow.

           :arg str workflow: unique name identifier of the workflow;
           :arg str list siteblacklist: black list of sites, with CMS name;
           :arg str list sitewhitelist: white list of sites, with CMS name."""
        # strict check on authz: only the workflow owner can modify it
        authz_owner_match(self.api, [workflow])
        return self.userworkflowmgr.resubmit(workflow=workflow, siteblacklist=siteblacklist, sitewhitelist=sitewhitelist, jobids=jobids, \
                                        userdn=cherrypy.request.headers['Cms-Authn-Dn'])

    @restcall
    def get(self, workflow, subresource, age, limit, shortformat, exitcode, jobids):
        """Retrieves the workflow information, like a status summary, in case the workflow unique name is specified.
           Otherwise returns all workflows since (now - age) for which the user is the owner.
           The caller needs to be a valid CMS user.

           :arg str workflow: unique name identifier of workflow;
           :arg int age: max workflow age in days;
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
                result = self.userworkflowmgr.status(workflow, userdn=userdn)
            # if have a subresource then it should be one of these
            elif subresource == 'logs':
                result = self.userworkflowmgr.logs(workflow, limit, exitcode, jobids, userdn=userdn)
            elif subresource == 'data':
                result = self.userworkflowmgr.output(workflow, limit, jobids, userdn=userdn)
            elif subresource == 'errors':
                result = self.userworkflowmgr.errors(workflow, shortformat)
            elif subresource == 'report':
                result = self.userworkflowmgr.report(workflow, userdn=userdn)
            # if here means that no valid subresource has been requested
            # flow should never pass through here since validation restrict this
            else:
                raise ExecutionError("Validation or method error")
        else:
            # retrieve the information about latest worfklows for that user
            # age can have a default: 1 week ?
            cherrypy.log("Found user '%s'" % cherrypy.request.user['login'])
            result = self.userworkflowmgr.getLatests(cherrypy.request.user['login'], limit, age)

        return result

    @restcall
    def delete(self, workflow, force, jobids):
        """Aborts a workflow. The user needs to be a CMS owner of the workflow.

           :arg str list workflow: list of unique name identifiers of workflows;
           :arg int force: force to delete the workflows in any case; 0 no, everything else yes;
           :return: nothing"""

        # strict check on authz: only the workflow owner can modify it
        authz_owner_match(self.api, [workflow])
        return self.userworkflowmgr.kill(workflow, force, jobids, userdn=cherrypy.request.headers['Cms-Authn-Dn'])
