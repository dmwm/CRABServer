# WMCore dependecies here
from WMCore.REST.Error import ExecutionError, InvalidParameter
from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Validation import validate_str, validate_strlist, validate_num

# CRABServer dependecies here
from CRABInterface.DataUserWorkflow import DataUserWorkflow
from CRABInterface.DataWorkflow import DataWorkflow
from CRABInterface.RESTExtensions import authz_owner_match, authz_login_valid
from CRABInterface.Regexps import *
from CRABInterface.Utils import CMSSitesCache, conn_handler

# external dependecies here
import cherrypy


class RESTUserWorkflow(RESTEntity):
    """REST entity for workflows from the user point of view and relative subresources"""

    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)

        self.userworkflowmgr = DataUserWorkflow()
        self.allCMSNames = CMSSitesCache(cachetime=0, sites={})

    def _checkSite(self, site):
        if site not in self.allCMSNames.sites and site not in DataWorkflow.sitewildcards:
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
            validate_strlist("siteblacklist", param, safe, RX_CMSSITE)
            [self._checkSite(site) for site in safe.kwargs['siteblacklist']]
            validate_strlist("sitewhitelist", param, safe, RX_CMSSITE)
            [self._checkSite(site) for site in safe.kwargs['sitewhitelist']]
            validate_strlist("blockwhitelist", param, safe, RX_BLOCK)
            validate_strlist("blockblacklist", param, safe, RX_BLOCK)
            validate_str("splitalgo", param, safe, RX_SPLIT, optional=False)
            validate_num("algoargs", param, safe, optional=False)
            validate_str("configdoc", param, safe, RX_CFGDOC, optional=False)
            validate_str("userisburl", param, safe, RX_CACHEURL, optional=False)
            validate_strlist("adduserfiles", param, safe, RX_ADDFILE)
            validate_strlist("addoutputfiles", param, safe, RX_ADDFILE)
            validate_num("savelogsflag", param, safe, optional=False)
            validate_str("publishname", param, safe, RX_PUBLISH, optional=False)
            validate_str("publishdbsurl", param, safe, RX_PUBDBSURL, optional=True)
            validate_str("asyncdest", param, safe, RX_CMSSITE, optional=False)
            self._checkSite(safe.kwargs['asyncdest'])
            validate_str("campaign", param, safe, RX_CAMPAIGN, optional=True)
            validate_num("blacklistT1", param, safe, optional=False)
            validate_str("dbsurl", param, safe, RX_DBSURL, optional=True)
            validate_str("acdcdoc", param, safe, RX_ACDCDOC, optional=True)
            validate_str("globaltag", param, safe, RX_GLOBALTAG, optional=True)
            validate_strlist("runs", param, safe, RX_RUNS)
            validate_strlist("lumis", param, safe, RX_LUMILIST)
            if len(safe.kwargs["runs"]) != len(safe.kwargs["lumis"]):
               raise InvalidParameter("The number of runs and the number of limi lists are different")

        elif method in ['POST']:
            validate_str("workflow", param, safe, RX_WORKFLOW, optional=False)
            validate_strlist("siteblacklist", param, safe, RX_CMSSITE)
            validate_strlist("sitewhitelist", param, safe, RX_CMSSITE)

        elif method in ['GET']:
            validate_str("workflow", param, safe, RX_WORKFLOW, optional=True)
            validate_str('subresource', param, safe, RX_SUBRESTAT, optional=True)
            #parameters of subresources calls has to be put here
            #used by get latest
            validate_num('age', param, safe, optional=True)
            #used by get log, get data
            validate_num('limit', param, safe, optional=True)
            validate_num('exitcode', param, safe, optional=True)
            if safe.kwargs['subresource'] not in ['data', 'logs', 'fwjr'] and (safe.kwargs['limit'] is not None or safe.kwargs['exitcode'] is not None):
                raise InvalidParameter("Invalid input parameters")
            #used by errors
            validate_num('shortformat', param, safe, optional=True)
            if safe.kwargs['subresource'] not in ['errors'] and safe.kwargs['shortformat'] is not None:
                raise InvalidParameter("Invalid input parameters")
            if not safe.kwargs['workflow'] and safe.kwargs['subresource']:
                raise InvalidParameter("Invalid input parameters")

        elif method in ['DELETE']:
            validate_str("workflow", param, safe, RX_WORKFLOW, optional=False)
            validate_num("force", param, safe, optional=True)


    @restcall
    def put(self, workflow, jobtype, jobsw, jobarch, inputdata, siteblacklist, sitewhitelist, blockwhitelist, blockblacklist,
            splitalgo, algoargs, configdoc, userisburl, adduserfiles, addoutputfiles, savelogsflag, publishname,
            asyncdest, campaign, blacklistT1, dbsurl, publishdbsurl, acdcdoc, globaltag, runs, lumis):
        """Insert a new workflow. The caller needs to be a CMS user with a valid CMS x509 cert/proxy.

           :arg str workflow: workflow name requested by the user;
           :arg str jobtype: job type of the workflow, usally CMSSW;
           :arg str jobsw: software requirement;
           :arg str jobarch: software architecture (=SCRAM_ARCH);
           :arg str inputdata: input dataset;
           :arg str list siteblacklist: black list of sites, with CMS name;
           :arg str list sitewhitelist: white list of sites, with CMS name;
           :arg str asyncdest: CMS site name for storage destination of the output files;
           :arg str list blockwhitelist: selective list of input iblock from the specified input dataset;
           :arg str list blockblacklist:  input blocks to be excluded from the specified input dataset;
           :arg str splitalgo: algorithm to be used for the workflow splitting;
           :arg str algoargs: argument to be used by the splitting algorithm;
           :arg str configdoc: the document id of the config cache document;
           :arg str userisburl: URL of the input sandbox file;
           :arg str list adduserfiles: list of additional input files;
           :arg str list addoutputfiles: list of additional output files;
           :arg int savelogsflag: archive the log files? 0 no, everything else yes;
           :arg str publishname: name to use for data publication;
           :arg str asyncdest: final destination of workflow output files;
           :arg str campaign: needed just in case the workflow has to be appended to an existing campaign;
           :arg int blacklistT1: flag enabling or disabling the black listing of Tier-1 sites;
           :arg str dbsurl: dbs url where the input dataset is published;
           :arg str publishdbsurl: dbs url where the output data has to be published;
           :arg str acdcdoc: input acdc document which contains the input information for data selction (eg: lumi mask);
           :arg str globaltag: the globaltag to use when running the job;
           :arg str runs: runs;
           :arg str lumis: lumis;
           :returns: a dict which contaians details of the submitted request"""

        return self.userworkflowmgr.submit(workflow=workflow, jobtype=jobtype, jobsw=jobsw, jobarch=jobarch, inputdata=inputdata,
                                       siteblacklist=siteblacklist, sitewhitelist=sitewhitelist, blockwhitelist=blockwhitelist,
                                       blockblacklist=blockblacklist, splitalgo=splitalgo, algoargs=algoargs, configdoc=configdoc,
                                       userisburl=userisburl, adduserfiles=adduserfiles, addoutputfiles=addoutputfiles,
                                       savelogsflag=savelogsflag, userdn=cherrypy.request.user['dn'], userhn=cherrypy.request.user['login'],
                                       publishname=publishname, asyncdest=asyncdest, campaign=campaign, blacklistT1=blacklistT1,
                                       dbsurl=dbsurl, publishdbsurl=publishdbsurl, acdcdoc=acdcdoc, globaltag=globaltag, runs=runs, lumis=lumis)

    @restcall
    def post(self, workflow, siteblacklist, sitewhitelist):
        """Resubmit an existing workflow. The caller needs to be a CMS user owner of the workflow.

           :arg str workflow: unique name identifier of the workflow;
           :arg str list siteblacklist: black list of sites, with CMS name;
           :arg str list sitewhitelist: white list of sites, with CMS name."""
        # strict check on authz: only the workflow owner can modify it
        authz_owner_match(self.userworkflowmgr, [workflow], retrieve_docs=False)
        self.userworkflowmgr.resubmit(workflow=workflow, siteblacklist=siteblacklist, sitewhitelist=sitewhitelist)
        return [{"result":"ok"}]

    @restcall
    def get(self, workflow, subresource, age, limit, shortformat, exitcode):
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
            # if have the wf then retrieve the wf status summary
            if not subresource:
                result = self.userworkflowmgr.status(workflow)
            # if have a subresource then it should be one of these
            elif subresource == 'logs':
                result = self.userworkflowmgr.logs(workflow, limit, exitcode)
            elif subresource == 'fwjr':
                result = self.userworkflowmgr.fwjr(workflow, limit, exitcode)
            elif subresource == 'data':
                result = self.userworkflowmgr.output(workflow, limit)
            elif subresource == 'errors':
                result = self.userworkflowmgr.errors(workflow, shortformat)
            elif subresource == 'report':
                result = self.userworkflowmgr.report(workflow)
            elif subresource == 'schema':
                result = rows([self.userworkflowmgr.schema(workflow)])
            elif subresource == 'configcache':
                result = rows([self.userworkflowmgr.configcache(workflow)])
            elif subresource == 'type':
                result = self.userworkflowmgr.getType(workflow)
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
    def delete(self, workflow, force):
        """Aborts a workflow. The user needs to be a CMS owner of the workflow.

           :arg str list workflow: list of unique name identifiers of workflows;
           :arg int force: force to delete the workflows in any case; 0 no, everything else yes;
           :return: nothing"""

        # strict check on authz: only the workflow owner can modify it
        authz_owner_match(self.userworkflowmgr, [workflow], retrieve_docs=False)
        self.userworkflowmgr.kill(workflow, force)
        return [{"result":"ok"}]
