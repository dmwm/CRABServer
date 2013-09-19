# WMCore dependecies here
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Validation import validate_str, validate_strlist, validate_num, validate_numlist
from WMCore.REST.Error import InvalidParameter

from Databases.TaskDB.Oracle.Task.ID import ID
from Databases.TaskDB.Oracle.Task.SetSplitargsTask import SetSplitargsTask
from Databases.TaskDB.Oracle.Task.GetReadyTasks import GetReadyTasks
from Databases.TaskDB.Oracle.Task.SetReadyTasks import SetReadyTasks
from Databases.TaskDB.Oracle.Task.SetStatusTask import SetStatusTask
from Databases.TaskDB.Oracle.Task.SetFailedTasks import SetFailedTasks
from Databases.TaskDB.Oracle.Task.SetInjectedTasks import SetInjectedTasks
from Databases.TaskDB.Oracle.Task.UpdateWorker import UpdateWorker
from Databases.TaskDB.Oracle.JobGroup.AddJobGroup import AddJobGroup
from Databases.TaskDB.Oracle.JobGroup.GetJobGroupFromJobDef import GetJobGroupFromJobDef

from CRABInterface.RESTExtensions import authz_login_valid
from CRABInterface.Regexps import RX_WORKFLOW, RX_BLOCK, RX_WORKER_NAME, RX_STATUS, RX_TEXT_FAIL, RX_DN, RX_SUBPOSTWORKER, RX_SUBGETWORKER, RX_RUNS, RX_LUMIRANGE

# external dependecies here
import cherrypy
from ast import literal_eval
from base64 import b64decode


class RESTWorkerWorkflow(RESTEntity):
    """REST entity to handle interactions between CAFTaskWorker and TaskManager database"""

    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)

    def validate(self, apiobj, method, api, param, safe):
        """Validating all the input parameter as enforced by the WMCore.REST module"""
        authz_login_valid()

        if method in ['PUT']:
            validate_str("workflow", param, safe, RX_WORKFLOW, optional=False)
            validate_num("subjobdef", param, safe, optional=True)
            validate_str("substatus", param, safe, RX_STATUS, optional=False)
            validate_strlist("subblocks", param, safe, RX_BLOCK)
            validate_str("subfailure", param, safe, RX_TEXT_FAIL, optional=True)
            validate_str("subuser", param, safe, RX_DN, optional=False)
        elif method in ['POST']:
            validate_str("workflow", param, safe, RX_WORKFLOW, optional=True)
            validate_str("status", param, safe, RX_STATUS, optional=True)
            validate_str("getstatus", param, safe, RX_STATUS, optional=True)
            validate_num("jobset", param, safe, optional=True)
            validate_str("failure", param, safe, RX_TEXT_FAIL, optional=True)
            validate_numlist("resubmittedjobs", param, safe)
            validate_str("workername", param, safe, RX_WORKER_NAME, optional=True)
            validate_str("subresource", param, safe, RX_SUBPOSTWORKER, optional=True) 
            validate_num("limit", param, safe, optional=True)
            validate_strlist("runs", param, safe, RX_RUNS)
            validate_strlist("lumis", param, safe, RX_LUMIRANGE)
            # possible combinations to check
            # 1) taskname + status
            # 2) taskname + status + failure
            # 3) taskname + status + resubmitted + jobsetid
            # 4) taskname + status == (1)
            # 5)            status + limit + getstatus + workername
            # 6) taskname + runs + lumis
        elif method in ['GET']:
            validate_str("workername", param, safe, RX_WORKER_NAME, optional=True)
            validate_str("getstatus", param, safe, RX_STATUS, optional=True)
            validate_num("limit", param, safe, optional=True)
            validate_str("subresource", param, safe, RX_SUBGETWORKER, optional=True)
            validate_num("subjobdef", param, safe, optional=True)
            validate_str("subuser", param, safe, RX_DN, optional=True)
            # possible combinations to check
            # 1) workername + getstatus + limit
            # 2) subresource + subjobdef + subuser
        elif method in ['DELETE']:
            pass

    @restcall
    def put(self, workflow, subjobdef, substatus, subblocks, subfailure, subuser):
        """ Insert a new jobgroup in the task"""
        if subfailure is not None:
            try:
                subfailure = b64decode(subfailure)
            except TypeError:
                raise InvalidParameter("Failure message is not in the accepted format")
        binds = {"task_name": [workflow], "jobdef_id": [subjobdef if subjobdef >= 0 else None], "jobgroup_status": [substatus], "blocks": [str(subblocks)],
                 "jobgroup_failure": [subfailure], "tm_user_dn": [subuser]}
        self.api.modify(AddJobGroup.sql, **binds)
        return []

    @restcall
    def post(self, workflow, status, subresource, jobset, failure, resubmittedjobs, getstatus, workername, limit, runs, lumis):
        """ Updates task information """
        if failure is not None:
            try:
                failure = b64decode(failure)
            except TypeError:
                raise InvalidParameter("Failure message is not in the accepted format")
        methodmap = {"state": {"args": (SetStatusTask.sql,), "method": self.api.modify, "kwargs": {"status": [status],
                                                                                       "taskname": [workflow]}},
                  "start": {"args": (SetReadyTasks.sql,), "method": self.api.modify, "kwargs": {"tm_task_status": [status],
                                                                                       "tm_taskname": [workflow]}},
                  "failure": {"args": (SetFailedTasks.sql,), "method": self.api.modify, "kwargs": {"tm_task_status": [status],
                                                                                "failure": [failure],
                                                                               "tm_taskname": [workflow]}},
                  "success": {"args": (SetInjectedTasks.sql,), "method": self.api.modify, "kwargs": {"tm_task_status": [status],
                                                                                            "panda_jobset_id": [jobset],
                                                                                            "tm_taskname": [workflow],
                                                                                            "resubmitted_jobs": [str(resubmittedjobs)]}},
                  "process": {"args": (UpdateWorker.sql,), "method": self.api.modifynocheck, "kwargs": {"tw_name": [workername],
                                                                                                   "get_status": [getstatus],
                                                                                                   "limit": [limit],
                                                                                                   "set_status": [status]}},
                  "lumimask": {"args": (runs, lumis,), "method": self.setLumiMask, "kwargs": {"taskname": [workflow],}},
        }

        if subresource is None:
            subresource = 'state'
        if not subresource in methodmap.keys():
            raise InvalidParameter("Subresource of workflowdb has not been found")
        methodmap[subresource]['method'](*methodmap[subresource]['args'], **methodmap[subresource]['kwargs'])
        return []

    @restcall
    def get(self, workername, getstatus, limit, subresource, subjobdef, subuser):
        """ Retrieve all columns for a specified task or
            tasks which are in a particular status with
            particular conditions """
        if subresource is not None and subresource == 'jobgroup':
            binds = {'jobdef_id': subjobdef, 'user_dn': subuser}
            rows = self.api.query(None, None, GetJobGroupFromJobDef.sql, **binds)
            for row in rows:
                # taskname, jobdefid, status, blocks, failures, dn
                yield {'tm_taskname': row[0],
                       'panda_jobdef_id': row[1],
                       'panda_jobdef_status': row[2],
                       'tm_data_blocks': literal_eval(row[3] if row[3] is None else row[3].read()),
                       'panda_jobgroup_failure': row[4] if row[4] is None else row[4].read(),
                       'tm_user_dn': row[5]}
        else:
            binds = {"limit": limit, "tw_name": workername, "get_status": getstatus}
            rows = self.api.query(None, None, GetReadyTasks.sql, **binds)
            for row in rows:
                newtask = Task()
                newtask.deserialize(row)
                yield dict(newtask)

    @restcall
    def delete(self):
        """ Delete a task from the DB """
        raise NotImplementedError

    def setLumiMask(self, runs, lumis, **binds):
        """ Load the old splitargs, convert it into the corresponding dict, and change runs and lumis
            accordingly to what the TaskWorker provided
        """
        #load the task
        task = self.api.query(None, None, ID.sql, taskname=binds['taskname'][0]).next()
        splitargs = literal_eval(task[6].read())
        #update the tm_splitargs
        splitargs['runs'] = runs
        splitargs['lumis'] = lumis
        binds['splitargs'] = [str(splitargs)]
        self.api.modify(SetSplitargsTask.sql, **binds)


class Task(dict):
    """Main object of work. This will be passed from a class to another.
       This will collect all task parameters contained in the DB, but
       living only in memory.

       NB: this can be reviewd and expanded in order to implement
           the needed methods to work with the database."""

    def __init__(self, *args, **kwargs):
        """Initializer of the task object.

           :arg *args/**kwargs: key/value pairs to update the dictionary."""
        self.update(*args, **kwargs)

    def deserialize(self, task):
        """Deserialize a task from a list format to the self Task dictionary.
           It depends on the order of elements, as they are returned from the DB.

           :arg list object task: the list of task attributes retrieved from the db."""
        self['tm_taskname'] = task[0]
        self['panda_jobset_id'] = task[1]
        self['tm_task_status'] = task[2]
        self['tm_start_time'] = str(task[3])
        self['tm_start_injection'] = str(task[4])
        self['tm_end_injection'] = str(task[5])
        self['tm_task_failure'] = task[6] if task[6] is None else task[6].read()
        self['tm_job_sw'] = task[7]
        self['tm_job_arch'] = task[8]
        self['tm_input_dataset'] = task[9]
        self['tm_site_whitelist'] = literal_eval(task[10])
        self['tm_site_blacklist'] = literal_eval(task[11])
        self['tm_split_algo'] = task[12]
        self['tm_split_args'] = literal_eval(task[13] if task[13] is None else task[13].read())
        self['tm_totalunits'] = task[14]
        self['tm_user_sandbox'] = task[15]
        self['tm_cache_url'] = task[16]
        self['tm_username'] = task[17]
        self['tm_user_dn'] = task[18]
        self['tm_user_vo'] = task[19]
        self['tm_user_role'] = task[20]
        self['tm_user_group'] = task[21]
        self['tm_publish_name'] = task[22]
        self['tm_asyncdest'] = task[23]
        self['tm_dbs_url'] = task[24]
        self['tm_publish_dbs_url'] = task[25]
        self['tm_publication'] = task[26]
        self['tm_outfiles'] = literal_eval(task[27])
        self['tm_tfile_outfiles'] = literal_eval(task[28])
        self['tm_edm_outfiles'] = literal_eval(task[29])
        self['tm_transformation'] = task[30]
        self['tm_job_type'] = task[31]
        extraargs = literal_eval(task[32] if task[32] is None else task[32].read())
        self['resubmit_site_whitelist'] = extraargs['siteWhiteList'] if 'siteWhiteList' in extraargs else []
        self['resubmit_site_blacklist'] = extraargs['siteBlackList'] if 'siteBlackList' in extraargs else []
        self['resubmit_ids'] = extraargs['resubmitList'] if 'resubmitList' in extraargs else []
        self['kill_ids'] = extraargs['killList'] if 'killList' in extraargs else []
        self['kill_all'] = extraargs['killAll'] if 'killAll' in extraargs else False
        self['panda_resubmitted_jobs'] = literal_eval(task[33] if task[33] is None else task[33].read())
        self['tm_save_logs'] = task[34]
        self['worker_name'] = task[35]
