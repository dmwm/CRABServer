""" This module is used to validate requests to the /workflow resource and calls either HTCondorDataWorkflow or DataWorkflow methods
"""
# avoid pylint complainging about non-elegfant code . It is old and works. Do not touch.
# pylint: disable=too-many-locals, too-many-branches, too-many-nested-blocks, too-many-statements, too-many-arguments


# external dependecies here
import logging
import cherrypy
# WMCore dependecies here
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Error import ExecutionError, InvalidParameter
from WMCore.REST.Validation import validate_str, validate_strlist, validate_num, validate_real
from WMCore.Services.TagCollector.TagCollector import TagCollector
from WMCore.Lexicon import userprocdataset, userProcDSParts, primdataset

# CRABServer dependecies here
from CRABInterface.DataUserWorkflow import DataUserWorkflow
from CRABInterface.RESTExtensions import authz_owner_match
from CRABInterface.Regexps import (RX_TASKNAME, RX_ACTIVITY, RX_JOBTYPE, RX_GENERATOR, RX_LUMIEVENTS, RX_CMSSW,
                                   RX_ARCH, RX_MICROARCH, RX_DATASET, RX_CMSSITE, RX_SPLIT, RX_CACHENAME,
                                   RX_LFN, RX_USERFILE, RX_VOPARAMS, RX_DBSURL, RX_LFNPRIMDS, RX_OUTFILES,
                                   RX_RUNS, RX_LUMIRANGE, RX_SCRIPTARGS, RX_SCHEDD_NAME, RX_COLLECTOR, RX_SUBRESTAT,
                                   RX_JOBID, RX_ADDFILE, RX_ANYTHING, RX_USERNAME, RX_DATE, RX_MANYLINES_SHORT,
                                   RX_CUDA_VERSION, RX_BLOCK, RX_RUCIODID, RX_RUCIOSCOPE)
from CRABInterface.Utilities import conn_handler, getDBinstance, validate_dict
from ServerUtilities import checkOutLFN, generateTaskName


class RESTUserWorkflow(RESTEntity):
    """REST entity for workflows from the user point of view and relative subresources"""

    def __init__(self, app, api, config, mount, centralcfg):
        RESTEntity.__init__(self, app, api, config, mount)

        self.logger = logging.getLogger("CRABLogger.RESTUserWorkflow")
        self.userworkflowmgr = DataUserWorkflow()
        self.centralcfg = centralcfg
        self.task = getDBinstance(config, 'TaskDB', 'Task')
        self.tagCollector = TagCollector(logger = self.logger, anytype = 1, anyarch = 1)

    @staticmethod
    def checkOutLFN(kwargs, username):
        """Check the lfn parameter: it must start with '/store/user/<username>/', '/store/group/groupname/' or '/store/local/something/',
           where username is the one registered in CMS global list (i.e. the one used in the CERN primary account).
           If lfn is not there, default to '/store/user/<username>/'.
        """
        if not kwargs['lfn']:
            ## Default to '/store/user/<username>/' if the user did not specify the lfn parameter.
            kwargs['lfn'] = f"/store/user/{username}/"
        else:
            if not checkOutLFN(kwargs['lfn'], username):
                msg = "The parameter Data.outLFNDirBase in the CRAB configuration file must start with either"
                msg += " '/store/user/<username>/' or '/store/group/<groupname>/'"
                msg += " (or '/store/local/<something>/' if publication is off),"
                msg += " where username is your username as registered in CMS services"
                msg += " (i.e. the username of your CERN primary account)."
                raise InvalidParameter(msg)

    def checkPublishDataName(self, kwargs, outlfn, requestname, username):
        """
        Validate the (user specified part of the) output dataset name for publication
        using the WMCore.Lexicon method userprocdataset(), which does the same
        validation as DBS (see discussion with Yuyi Guo in following github CRABClient
        issue: https://github.com/dmwm/CRABClient/issues/4257).

        This function will get deprecated in the 1509 release. We are keeping it around
        for backward compatibility but I would like to remove it at some point
        """
        if 'publishname' not in kwargs:
            ## This now can be undefined since new clients use publishname2
            msg = "Server parameter 'publishname' is not defined."
            msg += " It is possible that old client is being used."
            self.logger.info(msg)
            return
        ## The client defines kwargs['publishname'] = <Data.outputDatasetTag>-<isbchecksum>
        ## if the user defines Data.outputDatasetTag, and kwargs['publishname'] = <isbchecksum> otherwise.
        ## (The PostJob replaces then the isbchecksum by the psethash.)
        ## Here we add the requestname if the user did not specify the publishname
        if kwargs['publishname'].find('-') == -1:
            outputDatasetTagToCheck = f"{requestname.replace(':', '_')}-{kwargs['publishname']}"
        else:
            outputDatasetTagToCheck = f"{kwargs['publishname']}"
        kwargs['publishname'] = outputDatasetTagToCheck #that's what the version earlier than 1509 were putting in the DB
        if 'publishgroupname' in kwargs and int(kwargs['publishgroupname']): #the first half of the if is for backward compatibility
            if not (outlfn.startswith('/store/group/') and outlfn.split('/')[3]):
                msg = "Parameter 'publishgroupname' is True,"
                msg += " but parameter 'lfn' does not start with '/store/group/<groupname>'."
                raise InvalidParameter(msg)
            groupUserPrefix = outlfn.split('/')[3]
        else:
            groupUserPrefix = username
        outputDatasetTagToCheck = f"{groupUserPrefix}-{outputDatasetTagToCheck}"
        try:
            userprocdataset(outputDatasetTagToCheck)
        except AssertionError:
            ## The messages below are more descriptive than if we would use
            ## the message from AssertionError exception.
            if kwargs['publishname'].find('-') == -1:
                param = 'General.requestName'
                extrastr = 'crab_'
            else:
                param = 'Data.outputDatasetTag'
                extrastr = ''
            msg = f"Invalid CRAB configuration parameter {param}."
            msg += f" The combined string '{groupUserPrefix}-{extrastr}<{param}>'"
            msg += " should not have more than 166 characters"
            msg += f" and should match the regular expression {userProcDSParts['publishdataname']}"
            raise InvalidParameter(msg) from AssertionError

    ## Basically copy and pasted from checkPublishDataName which will be eventually removed
    @staticmethod
    def checkPublishDataName2(kwargs, requestname, username):
        """
        Validate the (user specified part of the) output dataset name for publication
        using the WMCore.Lexicon method userprocdataset(), which does the same
        validation as DBS (see discussion with Yuyi Guo in following github CRABClient
        issue: https://github.com/dmwm/CRABClient/issues/4257).
        """

        if 'publishname2' not in kwargs or not kwargs['publishname2']:
            ## provide the default publication name if it was not specified in the client
            outputDatasetTagToCheck = requestname.replace(':', '_')
        else:
            outputDatasetTagToCheck = kwargs['publishname2']

        ## Add the isbchecksum placeholder
        outputDatasetTagToCheck += "-" + "0" * 32

        #saves that in kwargs since it's what we want
        kwargs['publishname2'] = outputDatasetTagToCheck

        outputDatasetTagToCheck = f"{username}-{outputDatasetTagToCheck}"
        try:
            # check validity using WMCore's Lexicon
            userprocdataset(outputDatasetTagToCheck)
        except AssertionError:
            ## The messages below are more descriptive than if we would use
            ## the message from AssertionError exception.
            if not kwargs['publishname2']:
                param = 'General.requestName'
                extrastr = 'crab_'
            else:
                param = 'Data.outputDatasetTag'
                extrastr = ''
            msg = f"Invalid CRAB configuration parameter {param}."
            msg += f" The combined string '{username}-{extrastr}<{param}>'"
            msg += " should not have more than 166 characters"
            msg += f" and should match the regular expression {userProcDSParts['publishdataname']}"
            raise InvalidParameter(msg) from AssertionError

    @staticmethod
    def checkPrimaryDataset(kwargs, optional=False):
        """
        Validate the primary dataset name using the WMCore.Lexicon method primdataset(),
        which does the same validation as DBS (see discussion with Yuyi Guo in following
        github CRABClient issue: https://github.com/dmwm/CRABClient/issues/4257).
        """
        if 'primarydataset' not in kwargs:
            if optional:
                return
            msg = "Missing 'primarydataset' parameter."
            raise InvalidParameter(msg)
        try:
            primdataset(kwargs['primarydataset'])
        except AssertionError:
            ## This message is more descriptive than if we would use the message
            ## from AssertionError exception, but I had to explicitely write the
            ## regular expression [a-zA-Z][a-zA-Z0-9\-_]*, which is not nice.
            ## The message from AssertionError exception would be:
            ## "'<kwargs['primarydataset']>' does not match regular expression [a-zA-Z0-9\.\-_]+".
            msg = "Invalid 'primarydataset' parameter."
            msg += " The parameter should not have more than 99 characters"
            msg += " and should match the regular expression "
            msg += r"[a-zA-Z][a-zA-Z0-9\-_]*"
            raise InvalidParameter(msg) from AssertionError

    def checkReleases(self, jobarch, jobsw):
        """ Check if the software needed by the user is available in the tag collector
            Uses allScramArchsAndVersions from WMCore. If an IOError is raised report an error message.
            If the list of releases is empty (reason may be an ExpatError) then report an error message
            If the asked released is not there then report an error message
        """
        msg = False
        goodReleases = {}
        try:
            goodReleases = self.tagCollector.releases_by_architecture()
        except: #pylint: disable=bare-except
            msg = f"Error connecting to {self.tagCollector['endpoint']} (params: {self.tagCollector.tcArgs})"
            msg += " and determining the list of available releases. "
            msg += "Skipping the check of the releases"
        else:
            if goodReleases == {}:
                msg = f"The list of releases at {self.tagCollector['endpoint']} (params: {self.tagCollector.tcArgs})"
                msg += "is empty. Skipping the check of the releases"
            elif jobarch not in goodReleases or jobsw not in goodReleases[jobarch]:
                msg = f"ERROR: {jobsw} on {jobarch} is not among supported releases"
                msg += "\nUse config.JobType.allowUndistributedCMSSW = True if you are sure of what you are doing"
                excasync = f"ERROR: {jobsw} on {jobarch} is not among supported releases or an error occurred"
                invalidp = InvalidParameter(msg, errobj=excasync)
                setattr(invalidp, 'trace', '')
                raise invalidp

        if msg:
            #Need to log the message in the db for the users
            self.logger.warning(msg)

    @conn_handler(services=['centralconfig'])
    def validate(self, apiobj, method, api, param, safe): #pylint: disable=unused-argument
        """Validating all the input parameter as enforced by the WMCore.REST module"""

        if method in ['PUT']:
            username = cherrypy.request.user['login'] # username registered in CMS WEB frontend
            requestname = param.kwargs['workflow']
            param.kwargs['workflow'] = generateTaskName(username, requestname)
            validate_str("workflow", param, safe, RX_TASKNAME, optional=False)
            validate_str("activity", param, safe, RX_ACTIVITY, optional=True)
            validate_str("jobtype", param, safe, RX_JOBTYPE, optional=False)
            validate_str("generator", param, safe, RX_GENERATOR, optional=True)
            validate_str("eventsperlumi", param, safe, RX_LUMIEVENTS, optional=True)
            validate_str("jobsw", param, safe, RX_CMSSW, optional=False)
            validate_num("nonprodsw", param, safe, optional=False)
            validate_str("jobarch", param, safe, RX_ARCH, optional=False)
            validate_str("jobminuarch", param, safe, RX_MICROARCH, optional=True)
            if not safe.kwargs["nonprodsw"]: #if the user wants to allow non-production releases
                self.checkReleases(safe.kwargs['jobarch'], safe.kwargs['jobsw'])
            validate_num("useparent", param, safe, optional=True)
            validate_str("secondarydata", param, safe, RX_DATASET, optional=True)
            # site black/white list needs to be cast to unicode for later use in self._expandSites
            validate_strlist("siteblacklist", param, safe, RX_CMSSITE)
            safe.kwargs['siteblacklist'] = safe.kwargs['siteblacklist']
            validate_strlist("sitewhitelist", param, safe, RX_CMSSITE)
            safe.kwargs['sitewhitelist'] = safe.kwargs['sitewhitelist']
            validate_str("splitalgo", param, safe, RX_SPLIT, optional=False)
            validate_num("algoargs", param, safe, optional=False)
            try:
                validate_num("totalunits", param, safe, optional=True)
            except InvalidParameter:
                validate_real("totalunits", param, safe, optional=True)
            validate_str("cachefilename", param, safe, RX_CACHENAME, optional=False)
            validate_str("debugfilename", param, safe, RX_CACHENAME, optional=True)
            validate_str("lfn", param, safe, RX_LFN, optional=True)
            self.checkOutLFN(safe.kwargs, username)
            validate_strlist("addoutputfiles", param, safe, RX_ADDFILE, custom_err="Incorrect 'JobType.outputFiles' parameter. " \
                    f"Allowed regexp for each filename: '{RX_ADDFILE.pattern}'.")
            validate_strlist("userfiles", param, safe, RX_USERFILE, custom_err="Incorrect 'Data.userInputFiles' parameter. " \
                    f"Allowed regexp for each filename: '{ RX_USERFILE.pattern}'.")
            validate_num("savelogsflag", param, safe, optional=False)
            validate_num("saveoutput", param, safe, optional=True)
            validate_num("faillimit", param, safe, optional=True)
            validate_num("ignorelocality", param, safe, optional=True)
            validate_str("vorole", param, safe, RX_VOPARAMS, optional=True)
            validate_str("vogroup", param, safe, RX_VOPARAMS, optional=True)
            validate_num("publication", param, safe, optional=False)
            validate_str("publishdbsurl", param, safe, RX_DBSURL, optional=(not bool(safe.kwargs['publication'])))

            ## We might want to remove publishname once the backward compatibility
            ## wont be needed anymore. Then we can just keep publishname2
            ## NB: AFAIK the only client not using the CRABLibrary but direct cutl is HC,
            ## therefore we will need to make sure we do not break it!
            ## The following two lines will be removed in the future once we will
            ## not need backward compatibility anymore
            self.checkPublishDataName(param.kwargs, safe.kwargs['lfn'], requestname, username)
            validate_str('publishname', param, safe, RX_ANYTHING, optional=True)

            ##And this if as well, just do self.checkPublishDataName2(param.kwargs, safe.kwargs['lfn'], requestname, username)
            if not safe.kwargs["publishname"]: #new clients won't define this anymore
                ## The (user specified part of the) publication dataset name must be
                ## specified and must pass DBS validation. Since this is the correct
                ## validation function, it must be done before the
                ## validate_str("publishname", ...) we have below.
                self.checkPublishDataName2(param.kwargs, requestname, username)
            else:
                param.kwargs["publishname2"] = safe.kwargs["publishname"]

            ## 'publishname' was already validated above in checkPublishDataName().
            ## Calling validate_str with a fake regexp to move the param to the
            ## list of validated inputs
            validate_str("publishname2", param, safe, RX_ANYTHING, optional=True)

            if safe.kwargs['jobtype'] == 'PrivateMC':
                if param.kwargs.get('inputdata', None) or 'inputblocks' in param.kwargs:
                    msg = "Invalid 'inputdata' parameter."
                    msg += " Job type PrivateMC does not take any input dataset."
                    msg += " If you really intend to run over an input dataset, then you must use job type Analysis."
                    raise InvalidParameter(msg)
                if safe.kwargs['userfiles']:
                    msg = "Invalid 'userfiles' parameter."
                    msg += " Job type PrivateMC does not take any input files."
                    msg += " If you really intend to run over input files, then you must use job type Analysis."
                    raise InvalidParameter(msg)

            ## Client versions < 3.3.1511 may put in the input dataset something that is not
            ## really an input dataset (for PrivateMC or user input files). So the only case
            ## in which we are sure that we have to validate the input dataset is when the
            ## workflow type is Analysis, the workflow does not run on user input files and
            ## an input dataset is defined (scriptExe may not define an input).
            ## Once we don't care anymore about backward compatibility with client < 3.3.1511,
            ## we can uncomment the 1st line below and delete the next 4 lines.
            #validate_str("inputdata", param, safe, RX_DATASET, optional=True)
            if safe.kwargs['jobtype'] == 'Analysis' and param.kwargs.get('inputdata', None):  # beware!  #8372
                inputDataFromRucio = ':' in param.kwargs['inputdata']  # scope:name format
                if inputDataFromRucio:
                    validate_str("inputdata", param, safe, RX_RUCIODID, optional=True)
                    scope = safe.kwargs["inputdata"].split(':')[0]
                    containerName = safe.kwargs["inputdata"].split(':')[1]
                    if not RX_RUCIOSCOPE.match(scope):
                        raise InvalidParameter(f"Invalid Rucio scope: {scope}")
                    if not RX_DATASET.match(containerName):
                        raise InvalidParameter(f"Invalid dataset name: {containerName}")
                else:
                    validate_str("inputdata", param, safe, RX_DATASET, optional=True)
            else:
                validate_str("inputdata", param, safe, RX_ANYTHING, optional=True)

            ## The client is not forced to define the primary dataset. So make sure to have
            ## defaults or take it from the input dataset. The primary dataset is needed for
            ## the LFN of the output/log files and for publication. We want to have it well
            ## defined even if publication and/or transfer to storage are off.
            if safe.kwargs['inputdata']:
                if inputDataFromRucio:
                    param.kwargs['primarydataset'] = containerName.split('/')[1]
                else:
                    param.kwargs['primarydataset'] = safe.kwargs['inputdata'].split('/')[1]
            if not param.kwargs.get('primarydataset', None):
                if safe.kwargs['jobtype'] == 'PrivateMC':
                    param.kwargs['primarydataset'] = "CRAB_PrivateMC"
                elif safe.kwargs['jobtype'] == 'Analysis' and safe.kwargs['userfiles']:
                    param.kwargs['primarydataset'] = "CRAB_UserFiles"
                else:
                    param.kwargs['primarydataset'] = "CRAB_NoInput"
            ## We validate the primary dataset agains DBS rules even if publication is off,
            ## because in the future we may want to give the possibility to users to publish
            ## a posteriori.
            self.checkPrimaryDataset(param.kwargs, optional=False)
            validate_str("primarydataset", param, safe, RX_LFNPRIMDS, optional=False)

            validate_num("nonvaliddata", param, safe, optional=True)
            #if one and only one between outputDatasetTag and publishDbsUrl is set raise an error (we need both or none of them)
            # asyncdest needs to be cast to unicode for later use in self.checkASODestination
            validate_str("asyncdest", param, safe, RX_CMSSITE, optional=False)

            validate_num("oneEventMode", param, safe, optional=True)
            validate_num("priority", param, safe, optional=True)
            validate_num("maxjobruntime", param, safe, optional=True)
            validate_num("numcores", param, safe, optional=True)
            validate_num("maxmemory", param, safe, optional=True)
            validate_str("dbsurl", param, safe, RX_DBSURL, optional=False)
            validate_strlist("tfileoutfiles", param, safe, RX_OUTFILES, custom_err="Incorrect tfileoutfiles parameter (TFileService). " \
                    f"Allowed regexp: '{RX_OUTFILES.pattern}'.")
            validate_strlist("edmoutfiles", param, safe, RX_OUTFILES, custom_err="Incorrect edmoutfiles parameter (PoolOutputModule). " \
                    f"Allowed regexp: '{RX_OUTFILES.pattern}'.")
            validate_strlist("runs", param, safe, RX_RUNS)
            validate_strlist("lumis", param, safe, RX_LUMIRANGE)
            if len(safe.kwargs["runs"]) != len(safe.kwargs["lumis"]):
                raise InvalidParameter("The number of runs and the number of lumis lists are different")
            validate_strlist("adduserfiles", param, safe, RX_ADDFILE)
            validate_str("scriptexe", param, safe, RX_ADDFILE, optional=True)
            validate_strlist("scriptargs", param, safe, RX_SCRIPTARGS)
            validate_str("scheddname", param, safe, RX_SCHEDD_NAME, optional=True)
            validate_str("collector", param, safe, RX_COLLECTOR, optional=True)
            validate_strlist("extrajdl", param, safe, RX_SCRIPTARGS)
            validate_num("dryrun", param, safe, optional=True)
            validate_num("ignoreglobalblacklist", param, safe, optional=True)
            validate_num("partialdataset", param, safe, optional=True)
            validate_num("requireaccelerator", param, safe, optional=True)
            # validate optional acceleratorparams
            if param.kwargs.get("acceleratorparams", None):
                if not safe.kwargs["requireaccelerator"]:
                    raise InvalidParameter("There are accelerator parameters but requireAccelerator is False")
                with validate_dict("acceleratorparams", param, safe) as (accParams, accSafe):
                    validate_num("GPUMemoryMB", accParams, accSafe, minval=0, optional=True)
                    validate_strlist("CUDACapabilities", accParams, accSafe, RX_CUDA_VERSION)
                    validate_str("CUDARuntime", accParams, accSafe, RX_CUDA_VERSION, optional=True)
            else:
                safe.kwargs["acceleratorparams"] = None
            # Validate Data.inputBlocks
            validate_strlist("inputblocks", param, safe, RX_BLOCK)
            # Reject it if the blocks are not from its dataset.
            if safe.kwargs["inputblocks"]:
                for block in safe.kwargs['inputblocks']:
                    if block.find(safe.kwargs['inputdata']) != 0:
                        msg = f'Block "{block}" is not from Dataset "{safe.kwargs["inputdata"]}"'
                        raise InvalidParameter(msg)


        elif method in ['POST']:
            validate_str("workflow", param, safe, RX_TASKNAME, optional=False)
            validate_str("subresource", param, safe, RX_SUBRESTAT, optional=True)
            validate_strlist('jobids', param, safe, RX_JOBID)
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
                safe.kwargs['siteblacklist'] = safe.kwargs['siteblacklist']
            if 'sitewhitelist' not in param.kwargs:
                safe.kwargs['sitewhitelist'] = None
            elif param.kwargs['sitewhitelist'] == 'empty':
                safe.kwargs['sitewhitelist'] = []
                del param.kwargs['sitewhitelist']
            else:
                validate_strlist("sitewhitelist", param, safe, RX_CMSSITE)
                safe.kwargs['sitewhitelist'] = safe.kwargs['sitewhitelist']
            validate_num("maxjobruntime", param, safe, optional=True)
            validate_num("maxmemory", param, safe, optional=True)
            validate_num("priority", param, safe, optional=True)
            validate_num("force", param, safe, optional=True)
            validate_num("publication", param, safe, optional=True)

        elif method in ['GET']:
            validate_str("workflow", param, safe, RX_TASKNAME, optional=True)
            validate_str('subresource', param, safe, RX_SUBRESTAT, optional=True)
            validate_str('username', param, safe, RX_USERNAME, optional=True)
            validate_str('timestamp', param, safe, RX_DATE, optional=True) ## inserted by eric

            ## Used to determine how much information to return to the client for status.
            ## also used by report to determine if it has to check job states
            validate_num("verbose", param, safe, optional=True)

            ## used by get log, get data
            validate_num('limit', param, safe, optional=True)
            validate_num('exitcode', param, safe, optional=True)
            validate_strlist('jobids', param, safe, RX_JOBID)

            ## used by errors and report (short format in report means we do not query DBS)
            validate_num('shortformat', param, safe, optional=True)

            ## validation parameters
            if not safe.kwargs['workflow'] and safe.kwargs['subresource']:
                raise InvalidParameter("Invalid input parameters")
            if safe.kwargs['subresource'] in ['data', 'logs'] and not safe.kwargs['limit'] and not safe.kwargs['jobids']:
                raise InvalidParameter("You need to specify the number of jobs to retrieve or their ids.")

        elif method in ['DELETE']:
            validate_str("workflow", param, safe, RX_TASKNAME, optional=False)
            validate_str("killwarning", param, safe, RX_MANYLINES_SHORT, optional=True)


    @restcall
    #@getUserCert(headers=cherrypy.request.headers)
    def put(self, workflow, activity, jobtype, jobsw, jobarch, jobminuarch, inputdata, primarydataset, nonvaliddata,
            useparent, secondarydata, generator, eventsperlumi,
            siteblacklist, sitewhitelist, splitalgo, algoargs, cachefilename, debugfilename, addoutputfiles,
            savelogsflag, publication, publishname, publishname2, asyncdest, dbsurl, publishdbsurl, vorole, vogroup,
            tfileoutfiles, edmoutfiles, runs, lumis,
            totalunits, adduserfiles, oneEventMode, maxjobruntime, numcores, maxmemory, priority, nonprodsw, lfn, saveoutput,
            faillimit, ignorelocality, userfiles, scriptexe, scriptargs, scheddname, extrajdl, collector, dryrun, ignoreglobalblacklist,
            partialdataset, requireaccelerator, acceleratorparams, inputblocks):
        """Perform the workflow injection

           :arg str workflow: request name defined by the user;
           :arg str activity: workflow activity type, default None;
           :arg str jobtype: job type of the workflow, usually Analysis;
           :arg str jobsw: software requirement;
           :arg str jobarch: software architecture (=SCRAM_ARCH);
           :arg str jobminuarch: minimum required microarchitecture (=SCRAM_MIN_SUPPORTED_MICROARCH);
           :arg str inputdata: input dataset;
           :arg str primarydataset: primary dataset;
           :arg str nonvaliddata: allow invalid input dataset;
           :arg int useparent: add the parent dataset as secondary input;
           :arg str secondarydata: optional secondary intput dataset;
           :arg str generator: event generator for MC production;
           :arg str eventsperlumi: how many events to generate per lumi;
           :arg str list siteblacklist: black list of sites, with CMS name;
           :arg str list sitewhitelist: white list of sites, with CMS name;
           :arg str splitalgo: algorithm to be used for the workflow splitting;
           :arg str algoargs: argument to be used by the splitting algorithm;
           :arg str cachefilename: name of the file inside the cache
           :arg str debugfilename: name of the debugFiles tarball inside the cache
           :arg str list addoutputfiles: list of additional output files;
           :arg int savelogsflag: archive the log files? 0 no, everything else yes;
           :arg int publication: flag enabling or disabling data publication;
           :arg str publishname: name to use for data publication; deprecated
           :arg str publishname2: name to use for data publication;
           :arg str asyncdest: CMS site name for storage destination of the output files;
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
           :arg int oneEventMode: flag enabling oneEventMode
           :arg int maxjobruntime: max job runtime, in minutes
           :arg int numcores: number of CPU cores required by job
           :arg int maxmemory: maximum amount of RAM required, in MB
           :arg int priority: priority of this task
           :arg int saveoutput: whether to perform stageout for a given output file
           :arg int faillimit: the maximum number of failed jobs which triggers a workflow abort.
           :arg int ignorelocality: whether to ignore file locality in favor of the whitelist.
           :arg str userfiles: The files to process instead of a DBS-based dataset.
           :arg str scriptexe: script to execute in place of cmsrun.
           :arg str scriptargs: arguments to be passed to the scriptexe script.
           :arg str scheddname: Schedd Name used for debugging.
           :arg str extrajdl: extra Job Description Language parameters to be added.
           :arg str collector: Collector Name used for debugging.
           :arg int dryrun: enable dry run mode (initialize but do not submit request).
           :arg str ignoreglobalblacklist: flag to ignore site blacklist from SiteSupport
           :arg str partialdataset: to submit even if dataset is partially on disk
           :arg str requireaccelerator: to require jobs to run on CPU with hardwre accelerators
           :arg dict acceleratorparams: key:value pairs to define which exact accelerators
           :arg str list inputblocks: the blocks to process instead of the full dataset
           :returns: a dict which contaians details of the request"""

        userConfig = {
            'partialdataset': True if partialdataset else False,
            'requireaccelerator': True if requireaccelerator else False,
            'acceleratorparams': acceleratorparams if acceleratorparams else None,
            'inputblocks': inputblocks if inputblocks else None,
        }

        cacheurl = f"S3/{self.config.s3_bucket}"   # record S3 bucket in task table
        return self.userworkflowmgr.submit(workflow=workflow, activity=activity, jobtype=jobtype, jobsw=jobsw, jobarch=jobarch,
                                           jobminuarch=jobminuarch, inputdata=inputdata, primarydataset=primarydataset,
                                           nonvaliddata=nonvaliddata, use_parent=useparent,
                                           secondarydata=secondarydata, generator=generator, events_per_lumi=eventsperlumi,
                                           siteblacklist=siteblacklist, sitewhitelist=sitewhitelist, splitalgo=splitalgo,
                                           algoargs=algoargs, cachefilename=cachefilename, debugfilename=debugfilename,
                                           cacheurl=cacheurl, addoutputfiles=addoutputfiles, userdn=cherrypy.request.user['dn'],
                                           username=cherrypy.request.user['login'], savelogsflag=savelogsflag,
                                           vorole=vorole, vogroup=vogroup,
                                           publication=publication, publishname=publishname, publishname2=publishname2,
                                           asyncdest=asyncdest, lfn=lfn,
                                           dbsurl=dbsurl, publishdbsurl=publishdbsurl,
                                           tfileoutfiles=tfileoutfiles, edmoutfiles=edmoutfiles, runs=runs, lumis=lumis,
                                           totalunits=totalunits, adduserfiles=adduserfiles, oneEventMode=oneEventMode,
                                           maxjobruntime=maxjobruntime, numcores=numcores, maxmemory=maxmemory, priority=priority,
                                           ignorelocality=ignorelocality, saveoutput=saveoutput, faillimit=faillimit,
                                           userfiles=userfiles, scriptexe=scriptexe, scriptargs=scriptargs,
                                           scheddname=scheddname, extrajdl=extrajdl, collector=collector, dryrun=dryrun,
                                           submitipaddr=cherrypy.request.headers['X-Forwarded-For'],
                                           ignoreglobalblacklist=ignoreglobalblacklist, user_config=userConfig)

    @restcall
    def post(self, workflow, subresource, publication, jobids, force, siteblacklist, sitewhitelist, maxjobruntime, maxmemory, priority):
        """Resubmit or continue an existing workflow. The caller needs to be a CMS user owner of the workflow.

           :arg str workflow: unique name identifier of the workflow;
           :arg str list siteblacklist: black list of sites, with CMS name;
           :arg str list sitewhitelist: white list of sites, with CMS name."""
        # strict check on authz: only the workflow owner can modify it
        authz_owner_match(self.api, [workflow], self.task)
        if not subresource or subresource == 'resubmit':
            return self.userworkflowmgr.resubmit(workflow=workflow,
                                                 publication=publication,
                                                 jobids=jobids,
                                                 force=force,
                                                 siteblacklist=siteblacklist,
                                                 sitewhitelist=sitewhitelist,
                                                 maxjobruntime=maxjobruntime,
                                                 maxmemory=maxmemory,
                                                 priority=priority,
                                                 userdn=cherrypy.request.headers['Cms-Authn-Dn'])
        if subresource == 'resubmit2':
            return self.userworkflowmgr.resubmit2(workflow=workflow,
                                                  publication=publication,
                                                  jobids=jobids,
                                                  siteblacklist=siteblacklist,
                                                  sitewhitelist=sitewhitelist,
                                                  maxjobruntime=maxjobruntime,
                                                  maxmemory=maxmemory,
                                                  priority=priority)
        if subresource == 'proceed':
            return self.userworkflowmgr.proceed(workflow=workflow)

        raise Exception(f"Invalid subresource '{subresource}' for RESTUserWorkflo/POST")  # pylint: disable=broad-exception-raised

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
            userdn = cherrypy.request.headers['Cms-Authn-Dn']
            # if have the wf then retrieve the wf status summary
            if not subresource:
                result = self.userworkflowmgr.status(workflow, verbose=verbose, userdn=userdn)
            # if have a subresource then it should be one of these
            elif subresource == 'logs':
                result = self.userworkflowmgr.logs(workflow, limit, exitcode, jobids, userdn=userdn)
            elif subresource == 'data':
                result = self.userworkflowmgr.output(workflow, limit, jobids, userdn=userdn)
            elif subresource == 'logs2':
                result = self.userworkflowmgr.logs2(workflow, limit, jobids)
            elif subresource == 'data2':
                result = self.userworkflowmgr.output2(workflow, limit, jobids)
            elif subresource == 'errors':
                result = self.userworkflowmgr.errors(workflow, shortformat)
            elif subresource == 'report':
                result = self.userworkflowmgr.report(workflow, userdn=userdn, usedbs=shortformat)
            elif subresource == 'report2':
                result = self.userworkflowmgr.report2(workflow, userdn=userdn, usedbs=shortformat)
            elif subresource == 'publicationstatus':
                result = self.userworkflowmgr.publicationStatus(workflow, username)
            elif subresource == 'taskads':
                result = self.userworkflowmgr.taskads(workflow)
            # if here means that no valid subresource has been requested
            # flow should never pass through here since validation restrict this
            else:
                raise ExecutionError("Validation or method error")
        else:
            # retrieve the information about latest worfklows for that user
            # age can have a default: 1 week ?
            cherrypy.log(f"Found user '{cherrypy.request.user['login']}'")
            result = self.userworkflowmgr.getLatests(username or cherrypy.request.user['login'], timestamp)     #eric added timestamp to match username

        return result

    @restcall
    def delete(self, workflow, killwarning=''):
        """Aborts a workflow. The user needs to be a CMS owner of the workflow.

           :arg str list workflow: list of unique name identifiers of workflows;
           :arg int force: force to delete the workflows in any case; 0 no, everything else yes;
           :return: nothing"""

        # strict check on authz: only the workflow owner can modify it
        authz_owner_match(self.api, [workflow], self.task)
        return self.userworkflowmgr.kill(workflow, killwarning)
