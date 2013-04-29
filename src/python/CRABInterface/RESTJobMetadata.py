# WMCore dependecies here
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Validation import validate_str

# CRABServer dependecies here
from CRABInterface.RESTExtensions import authz_login_valid
from CRABInterface.Regexps import RX_WORKFLOW, RX_OUTTYPES
from CRABInterface.DataJobMetadata import DataJobMetadata

# external dependecies here
import cherrypy


class RESTJobMetadata(RESTEntity):
    """REST entity to handle job metadata information"""

    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        self.jobmetadata = DataJobMetadata()

    def validate(self, apiobj, method, api, param, safe):
        """Validating all the input parameter as enforced by the WMCore.REST module"""
        authz_login_valid()

        if method in ['PUT']:
            raise NotImplementedError
        elif method in ['POST']:
            raise NotImplementedError
        elif method in ['GET']:
            validate_str("taskname", param, safe, RX_WORKFLOW, optional=False)
            validate_str("filetype", param, safe, RX_OUTTYPES, optional=False)
        elif method in ['DELETE']:
            raise NotImplementedError

    @restcall
    def put(self, taskname, outfilelumis, inparentlfns, globalTag, outfileruns,
            pandajobid, outsize, publishdataname, appver, outtype, checksummd5,
            checksumcksum, checksumadler32, outlocation, outdatasetname, outlfn,
            events):
        """Insert a new job metadata information"""
        raise NotImplementedError

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
