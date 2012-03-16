# WMCore dependecies here
from WMCore.REST.Error import ExecutionError
from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Validation import validate_str, validate_strlist, validate_num, validate_numlist

# CRABServer dependecies here
from CRABInterface.DataWorkflow import DataWorkflow
from CRABInterface.RESTExtensions import authz_owner_match, authz_login_valid
from CRABInterface.Regexps import *

# external dependecies here
import cherrypy


class RESTWorkflow(RESTEntity):
    """REST entity for workflows and relative subresources"""

    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)

        self.workflowmgr = DataWorkflow()

    def validate(self, apiobj, method, api, param, safe):
        """Validating all the input parameter as enforced by the WMCore.REST module"""
        authz_login_valid()

        if method in ['PUT']:
            validate_str("workflow", param, safe, RX_WORKFLOW, optional=False)
            validate_str("jobtype", param, safe, RX_JOBTYPE, optional=False)
            validate_str("jobsw", param, safe, RX_CMSSW, optional=False)
            validate_str("jobarch", param, safe, RX_ARCH, optional=False)
            validate_str("inputdata", param, safe, RX_DATASET, optional=False)
            validate_strlist("siteblacklist", param, safe, RX_CMSSITE)
            validate_strlist("sitewhitelist", param, safe, RX_CMSSITE)
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
            validate_str("asyncdest", param, safe, RX_CMSSITE, optional=False)
            validate_str("campaign", param, safe, RX_CAMPAIGN, optional=True)
            validate_num("blacklistT1", param, safe, optional=False)

        elif method in ['POST']:
            validate_str("workflow", param, safe, RX_WORKFLOW, optional=False)
            validate_num("resubmit", param, safe, optional=True)
            validate_str("dbsurl", param, safe, RX_DBSURL, optional=True)

        elif method in ['GET']:
            validate_strlist("workflow", param, safe, RX_WORKFLOW)
            validate_str('subresource', param, safe, RX_SUBRESTAT, optional=True)
            #parameters of subresources calls has to be put here
            #used by get latest
            validate_num('age', param, safe, optional=True)
            #used by get log, gt data
            validate_num('limit', param, safe, optional=True)
            #used by errors
            validate_num('shortformat', param, safe, optional=True)

        elif method in ['DELETE']:
            validate_strlist("workflow", param, safe, RX_WORKFLOW)
            validate_num("force", param, safe, optional=True)


    @restcall
    def put(self, workflow, jobtype, jobsw, jobarch, inputdata, siteblacklist, sitewhitelist, blockwhitelist, blockblacklist,
            splitalgo, algoargs, configdoc, userisburl, adduserfiles, addoutputfiles, savelogsflag, publishname, asyncdest, campaign, blacklistT1):
        """Insert a new workflow. The caller needs to be a CMS user with a valid CMS x509 cert/proxy.

           :arg str workflow: workflow name requested by the user;
           :arg str jobtype: job type of the workflow, usally CMSSW;
           :arg str jobsw: software requirement;
           :arg str jobarch: software architecture (=SCRAM_ARCH);
           :arg str list inputdata: input datasets;
           :arg str list siteblacklist: black list of sites, with CMS name;
           :arg str list sitewhitelist: white list of sites, with CMS name;
           :arg str asyncdest: CMS site name for storage destination of the output files;
           :arg str list blockwhitelist: selective list of input iblock from the specified input dataset;
           :arg str list blockblacklist:  input blocks to be excluded from the specified input dataset;
           :arg str splitalgo: algorithm to be used for the workflow splitting;
           :arg str algoargs: argument to be used by the splitting algorithm;
           :arg str configdoc: the document id of the config cache document:
           :arg str userisburl: URL of the input sandbox file;
           :arg str list adduserfiles: list of additional input files;
           :arg str list addoutputfiles: list of additional output files;
           :arg int savelogsflag: archive the log files? 0 no, everything else yes;
           :arg str publishname: name to use for data publication;
           :arg str asyncdest: final destination of workflow output files;
           :arg str campaign: needed just in case the workflow has to be appended to an existing campaign;
           :arg str userdn: the user DN
           :arg str configfile: configuration file provided by the user as string
           :arg str psettweaks: a json representing the psettweak provided by the user
           :arg str psethash: the hash od the psetfile
           :arg str label:
           :arg str  description:

           :returns: a dict which contaians details of the request"""

        return self.workflowmgr.submit(workflow=workflow, jobtype=jobtype, jobsw=jobsw, jobarch=jobarch, inputdata=inputdata,
                                       siteblacklist=siteblacklist, sitewhitelist=sitewhitelist, blockwhitelist=blockwhitelist,
                                       blockblacklist=blockblacklist, splitalgo=splitalgo, algoargs=algoargs, configdoc=configdoc,
                                       userisburl=userisburl, adduserfiles=adduserfiles, addoutputfiles=addoutputfiles,
                                       savelogsflag=savelogsflag, userdn=cherrypy.request.user['dn'], userhn=cherrypy.request.user['login'],
                                       publishname=publishname, asyncdest=asyncdest, campaign=campaign, blacklistT1=blacklistT1)

    @restcall
    def post(self, workflow, resubmit, dbsurl):
        """Modifies an existing workflow. The caller needs to be a CMS user owner of the workflow.

           :arg str workflow: unique name identifier of the workflow;
           :arg int resubmit: reubmit the workflow? 0 no, everything else yes;
           :arg str dbsurl: publish the workflow results
           :returns: return the modified fields or the publication details"""

        result = []
        if resubmit:
            # strict check on authz: only the workflow owner can modify it
            alldocs = authz_owner_match(self.workflowmgr.database, [workflow])
            result = rows([self.workflowmgr.resubmit(workflow)])
        elif dbsurl:
            result = rows([self.workflowmgr.publish(workflow, dbsurl)])

        return result

    @restcall
    def get(self, workflow, subresource, age, limit, shortformat):
        """Retrieves the workflows information, like a status summary, in case the workflow unique name is specified.
           Otherwise returns all workflows since (now - age) for which the user is the owner.
           The caller needs to be a CMS user owner of the workflow.

           :arg str list workflow: list of unique name identifiers of workflows;
           :arg int age: max workflows age in days;
           :arg str subresource: the specific workflow information to be accessed;
           :arg int limit: limit of return entries for some specific subresource;
           :retrun: the list of workflows with the relative status summary in case of per user request; or
                    the requested subresource."""

        result = []
        if workflow:
            # if have the wf then retrieve the wf status summary
            if not subresource:
                return self.workflowmgr.status(workflow)
            # if have a subresource then it should be one of these
            elif subresource == 'logs':
                result = self.workflowmgr.logs(workflow, limit)
            elif subresource == 'data':
                result = self.workflowmgr.output(workflow, limit)
            elif subresource == 'errors':
                result = self.workflowmgr.errors(workflow, shortformat)
            elif subresource == 'report':
                result = rows([self.workflowmgr.report(workflow)])
            elif subresource == 'schema':
                result = rows([self.workflowmgr.schema(workflow)])
            elif subresource == 'configcache':
                result = rows([self.workflowmgr.configcache(workflow)])
            # if here means that no valid subresource has been requested
            # flow should never pass through here since validation restrict this
            else:
                raise ExecutionError("Validation or method error")
        else:
            # retrieve the information about latest worfklows for that user
            # age can have a default: 1 week ?
            cherrypy.log("Found user '%s'" % cherrypy.request.user['login'])
            result = self.workflowmgr.getLatests(cherrypy.request.user['login'], limit, age)

        return result

    @restcall
    def delete(self, workflow, force):
        """Aborts a workflow. The user needs to be a CMS owner of the workflow.

           :arg str list workflow: list of unique name identifiers of workflows;
           :arg int force: force to delete the workflows in any case; 0 no, everything else yes;
           :return: nothing?"""

        # strict check on authz: only the workflow owner can modify it
        alldocs = authz_owner_match(self.workflowmgr.database, workflow)
        result = rows([self.workflowmgr.kill(workflow, force)])
        return result
