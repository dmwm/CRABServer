# WMCore dependecies here
from WMCore.REST.Error import InvalidParameter
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Validation import validate_str, validate_strlist, validate_num, validate_numlist

# CRABServer dependecies here
from CRABInterface.RESTExtensions import authz_owner_match, authz_login_valid
from CRABInterface.Regexps import *
from CRABInterface.DataFileMetadata import DataFileMetadata

# external dependecies here
import cherrypy


class RESTFileMetadata(RESTEntity):
    """REST entity to handle job metadata information"""

    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        self.jobmetadata = DataFileMetadata()

    def validate(self, apiobj, method, api, param, safe):
        """Validating all the input parameter as enforced by the WMCore.REST module"""
        authz_login_valid()

        if method in ['PUT']:
            #TODO check optional parameter
            #TODO check all the regexp
            validate_str("taskname", param, safe, RX_WORKFLOW, optional=False)
            validate_strlist("outfilelumis", param, safe, RX_LUMILIST)
            validate_numlist("outfileruns", param, safe)
            if len(safe.kwargs["outfileruns"]) != len(safe.kwargs["outfilelumis"]):
                raise InvalidParameter("The number of runs and the number of lumis lists are different")
            validate_strlist("inparentlfns", param, safe, RX_LFN)
            validate_str("globalTag", param, safe, RX_GLOBALTAG, optional=True)
            validate_num("pandajobid", param, safe, optional=False)
            validate_num("outsize", param, safe, optional=False)
            validate_str("publishdataname", param, safe, RX_PUBLISH, optional=False)
            validate_str("appver", param, safe, RX_CMSSW, optional=False)
            validate_str("outtype", param, safe, RX_OUTTYPES, optional=False)
            validate_str("checksummd5", param, safe, RX_CHECKSUM, optional=False)
            validate_str("checksumcksum", param, safe, RX_CHECKSUM, optional=False)
            validate_str("checksumadler32", param, safe, RX_CHECKSUM, optional=False)
            validate_str("outlocation", param, safe, RX_CMSSITE, optional=False)
            validate_str("outtmplocation", param, safe, RX_CMSSITE, optional=False)
            validate_str("acquisitionera", param, safe, RX_WORKFLOW, optional=False)#TODO Do we really need this?
            validate_str("outdatasetname", param, safe, RX_LFN, optional=False)#TODO temporary, need to come up with a regex
            validate_str("outlfn", param, safe, RX_LFN, optional=False)
            validate_num("events", param, safe, optional=False)
        elif method in ['POST']:
            raise NotImplementedError
        elif method in ['GET']:
            validate_str("taskname", param, safe, RX_WORKFLOW, optional=False)
            validate_str("filetype", param, safe, RX_OUTTYPES, optional=False)
        elif method in ['DELETE']:
            raise NotImplementedError

    @restcall
    def put(self, taskname, outfilelumis, inparentlfns, globalTag, outfileruns, pandajobid, outsize, publishdataname, appver, outtype, checksummd5,\
                checksumcksum, checksumadler32, outlocation, outtmplocation, outdatasetname, acquisitionera, outlfn, events):
        """Insert a new job metadata information"""
        return self.jobmetadata.inject(taskname=taskname, outfilelumis=outfilelumis, inparentlfns=inparentlfns, globalTag=globalTag, outfileruns=outfileruns,\
                           pandajobid=pandajobid, outsize=outsize, publishdataname=publishdataname, appver=appver, outtype=outtype, checksummd5=checksummd5,\
                           checksumcksum=checksumcksum, checksumadler32=checksumadler32, outlocation=outlocation, outtmplocation=outtmplocation,\
                           outdatasetname=outdatasetname, acquisitionera=acquisitionera, outlfn=outlfn, events=events)

    @restcall
    def post(self):
        """Modifies and existing job metadata information"""

        raise NotImplementedError

    @restcall
    def get(self, taskname, filetype):
        """Retrieves a specific job metadata information.

           :arg str taskname: unique name identifier of the task;
           :arg str filetype: filter the file type to return;
           :retrun: generator looping through the resulting db rows."""
        return self.jobmetadata.getFiles(taskname, filetype)

    @restcall
    def delete(self):
        """Deletes an existing job metadata information"""

        raise NotImplementedError
