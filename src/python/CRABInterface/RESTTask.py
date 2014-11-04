# WMCore dependecies here
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Validation import validate_str, validate_strlist, validate_num, validate_numlist
from WMCore.REST.Error import InvalidParameter,ExecutionError

from CRABInterface.Utils import getDBinstance
from CRABInterface.RESTExtensions import authz_login_valid, authz_owner_match
from CRABInterface.Regexps import RX_SUBRES_TASK, RX_WORKFLOW, RX_BLOCK, RX_WORKER_NAME, RX_STATUS, RX_USERNAME, RX_TEXT_FAIL, RX_DN, RX_SUBPOSTWORKER, RX_SUBGETWORKER, RX_RUNS, RX_LUMIRANGE, RX_OUT_DATASET, RX_URL, RX_OUT_DATASET

# external dependecies here
import cherrypy
from ast import literal_eval
from base64 import b64decode


class RESTTask(RESTEntity):
    """REST entity to handle interactions between CAFTaskWorker and TaskManager database"""

    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        self.Task = getDBinstance(config, 'TaskDB', 'Task')
        self.JobGroup = getDBinstance(config, 'TaskDB', 'JobGroup')

    def validate(self, apiobj, method, api, param, safe):
        """Validating all the input parameter as enforced by the WMCore.REST module"""
        authz_login_valid()
        if method in ['POST']:
            validate_str('subresource', param, safe, RX_SUBRES_TASK, optional=False)
            validate_str("workflow", param, safe, RX_WORKFLOW, optional=True)
            validate_str("warning", param, safe, RX_WORKFLOW, optional=True)
            validate_str("webdirurl", param, safe, RX_URL, optional=True)
            validate_strlist("outputdatasets", param, safe, RX_OUT_DATASET)
        elif method in ['GET']:
            validate_str('subresource', param, safe, RX_SUBRES_TASK, optional=False)
            validate_str("workflow", param, safe, RX_WORKFLOW, optional=True)
            validate_str('taskstatus', param, safe, RX_STATUS, optional=True)
            validate_str('username', param, safe, RX_USERNAME, optional=True)

    @restcall
    def get(self, subresource, **kwargs):
        """Retrieves the server information, like delegateDN, filecacheurls ...
           :arg str subresource: the specific server information to be accessed;
        """
        return getattr(RESTTask, subresource)(self, **kwargs)

    def allusers(self, **kwargs):
        rows = self.api.query(None, None, self.Task.ALLUSER_sql)
        return rows

    def allinfo(self, **kwargs):
        rows = self.api.query(None, None, self.Task.IDAll_sql, taskname=kwargs['workflow'])
        return rows

	#INSERTED BY ERIC SUMMER STUDENT
    def summary(self, **kwargs):
        """ Retrieves the data for list all users"""
        rows = self.api.query(None, None, self.Task.TASKSUMMARY_sql)
        return rows

    #Quick search api
    def search(self, **kwargs):
        """Retrieves specific data from task db"""
        rows = self.api.query(None, None, self.Task.QuickSearch_sql, taskname=kwargs["workflow"])
        return rows

    #Get all jobs with a specified status
    def taskbystatus(self, **kwargs):
        """Retrieves all jobs of the specified user with the specified status"""
        rows = self.api.query(None, None, self.Task.TaskByStatus_sql, username_=kwargs["username"], taskstatus=kwargs["taskstatus"])

        return rows

    @restcall
    def post(self, subresource, **kwargs):
        """ Updates task information """

        return getattr(RESTTask, subresource)(self, **kwargs)

    def addwarning(self, **kwargs):
        """ Add a warning to the wraning column in the database. Can be tested with:
            curl -X POST https://mmascher-poc.cern.ch/crabserver/dev/task -k --key /tmp/x509up_u8440 --cert /tmp/x509up_u8440 \
                    -d 'subresource=addwarning&workflow=140710_233424_crab3test-5:mmascher_crab_HCprivate12&warning=blahblah' -v
        """
        #check if the parameters are there
        if 'warning' not in kwargs or not kwargs['warning']:
            raise InvalidParameter("Warning message not found in the input parameters")
        if 'workflow' not in kwargs or not kwargs['workflow']:
            raise InvalidParameter("Task name not found in the input parameters")

        #decoding and setting the parameters
        workflow = kwargs['workflow']
        authz_owner_match(self.api, [workflow], self.Task) #check that I am modifying my own workflow
        try:
            warning = b64decode(kwargs['warning'])
        except TypeError:
            raise InvalidParameter("Failure message is not in the accepted format")

#        rows = self.api.query(None, None, "SELECT tm_task_warnings FROM tasks WHERE tm_taskname = :workflow", workflow=workflow)#self.Task.TASKSUMMARY_sql)
        rows = self.api.query(None, None, self.Task.ID_sql, taskname=workflow)#self.Task.TASKSUMMARY_sql)
        rows = list(rows) #from generator to list
        if len(rows)==0:
            raise InvalidParameter("Task %s not found in the task database" % workflow)

        row = self.Task.ID_tuple(*rows[0])
        warnings = literal_eval(row.task_warnings.read() if row.task_warnings else '[]')
        if len(warnings)>10:
            raise ExecutionError("You cannot add more than 10 warnings to a task")
        warnings.append(warning)

        self.api.modify(self.Task.SetWarnings_sql, warnings=[str(warnings)], workflow=[workflow])

        return []

    def addwebdir(self, **kwargs):
        """ Add web directory to web_dir column in the database. Can be tested with:
            curl -X POST https://balcas-crab.cern.ch/crabserver/dev/task -k --key $X509_USER_PROXY --cert $X509_USER_PROXY \
                    -d 'subresource=addwebdir&workflow=140710_233424_crab3test-5:mmascher_crab_HCprivate12&webdirurl=http://cmsweb.cern.ch/crabserver/testtask' -v
        """
        #check if the parameters are there
        if 'webdirurl' not in kwargs or not kwargs['webdirurl']:
            raise InvalidParameter("Web directory url not found in the input parameters")
        if 'workflow' not in kwargs or not kwargs['workflow']:
            raise InvalidParameter("Task name not found in the input parameters")

        workflow = kwargs['workflow']
        authz_owner_match(self.api, [workflow], self.Task) #check that I am modifying my own workflow

        self.api.modify(self.Task.UpdateWebUrl_sql, webdirurl=[str(kwargs['webdirurl'])], workflow=[workflow])

        return []

    def addoutputdatasets(self, **kwargs):
        if 'outputdatasets' not in kwargs or not kwargs['outputdatasets']:
            raise InvalidParameter("Output datasets not found in the input parameters")
        if 'workflow' not in kwargs or not kwargs['workflow']:
            raise InvalidParameter("Task name not found in the input parameters")

        workflow = kwargs['workflow']
        authz_owner_match(self.api, [workflow], self.Task) #check that I am modifying my own workflow

        row = self.Task.ID_tuple(*self.api.query(None, None, self.Task.ID_sql, taskname=workflow).next())
        outputdatasets = literal_eval(row.tm_output_dataset.read() if row.tm_output_dataset else '[]')
        outputdatasets = str(list(set(outputdatasets + kwargs['outputdatasets'])))

        self.api.modify(self.Task.SetUpdateOutDataset_sql, tm_output_dataset=[outputdatasets], tm_taskname=[workflow])

        return []
