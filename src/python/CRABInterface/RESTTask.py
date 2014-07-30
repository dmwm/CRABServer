# WMCore dependecies here
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Validation import validate_str, validate_strlist, validate_num, validate_numlist
from WMCore.REST.Error import InvalidParameter

from CRABInterface.Utils import getDBinstance
from CRABInterface.RESTExtensions import authz_login_valid
from CRABInterface.Regexps import RX_SUBRES_TASK, RX_WORKFLOW, RX_BLOCK, RX_WORKER_NAME, RX_STATUS, RX_USERNAME, RX_TEXT_FAIL, RX_DN, RX_SUBPOSTWORKER, RX_SUBGETWORKER, RX_RUNS, RX_LUMIRANGE, RX_OUT_DATASET

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
            validate_str("workflow", param, safe, RX_WORKFLOW, optional=True)
        elif method in ['GET']:
            validate_str('subresource', param, safe, RX_SUBRES_TASK, optional=False)
            validate_str('taskstatus', param, safe, RX_STATUS, optional=True)
            validate_str('username', param, safe, RX_USERNAME, optional=True)
            validate_str("workflow", param, safe, RX_WORKFLOW, optional=True)

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
    def post(self, workflow):
        """ Updates task information """
        if failure is not None:
            try:
                failure = b64decode(failure)
            except TypeError:
                raise InvalidParameter("Failure message is not in the accepted format")
        methodmap = {"state": {"args": (self.Task.SetStatusTask_sql,), "method": self.api.modify, "kwargs": {"status": [status],
                                                                                       "taskname": [workflow]}},
                  "start": {"args": (self.Task.SetReadyTasks_sql,), "method": self.api.modify, "kwargs": {"tm_task_status": [status],
                                                                                       "tm_taskname": [workflow]}},
                  "failure": {"args": (self.Task.SetFailedTasks_sql,), "method": self.api.modify, "kwargs": {"tm_task_status": [status],
                                                                                "failure": [failure],
                                                                               "tm_taskname": [workflow]}},
                  "success": {"args": (self.Task.SetInjectedTasks_sql,), "method": self.api.modify, "kwargs": {"tm_task_status": [status],
                                                                                            "panda_jobset_id": [jobset],
                                                                                            "tm_taskname": [workflow],
                                                                                            "resubmitted_jobs": [str(resubmittedjobs)]}},
                  "process": {"args": (self.Task.UpdateWorker_sql,), "method": self.api.modifynocheck, "kwargs": {"tw_name": [workername],
                                                                                                   "get_status": [getstatus],
                                                                                                   "limit": [limit],
                                                                                                   "set_status": [status]}},
                  "lumimask": {"args": (runs, lumis,), "method": self.setLumiMask, "kwargs": {"taskname": [workflow],}},
                  "outputdataset" :  {"args": (self.Task.SetUpdateOutDataset_sql,), "method": self.api.modify, "kwargs": {"tm_output_dataset": [str(outputdataset)],
                                                                                                                          "tm_taskname": [workflow]}},

        }

        if subresource is None:
            subresource = 'state'
        if not subresource in methodmap.keys():
            raise InvalidParameter("Subresource of workflowdb has not been found")
        methodmap[subresource]['method'](*methodmap[subresource]['args'], **methodmap[subresource]['kwargs'])
        return []

