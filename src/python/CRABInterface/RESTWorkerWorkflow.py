""" Interface used by the TaskWorker to ucquire tasks and change their state
"""
# WMCore dependecies here
from builtins import str
from Utils.Utilities import decodeBytesToUnicode
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Validation import validate_str, validate_strlist, validate_num
from WMCore.REST.Error import InvalidParameter

from ServerUtilities import getEpochFromDBTime
from CRABInterface.Utilities import getDBinstance
from CRABInterface.RESTExtensions import authz_login_valid
from CRABInterface.Regexps import (RX_MANYLINES_SHORT, RX_TASKNAME, RX_WORKER_NAME, RX_STATUS, RX_SUBPOSTWORKER,
                                  RX_JOBID)

# external dependecies here
from ast import literal_eval
import json


class RESTWorkerWorkflow(RESTEntity):
    """REST entity to handle interactions between CAFTaskWorker and TaskManager database"""

    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        self.Task = getDBinstance(config, 'TaskDB', 'Task')

    @staticmethod
    def validate(apiobj, method, api, param, safe): #pylint: disable=unused-argument
        """Validating all the input parameter as enforced by the WMCore.REST module"""
        authz_login_valid() #TODO: should we also call authz_operator here ? Otherwise anybody can get tasks from here.
                            #      Actually, maybe something even more strict is necessary (only the prod TW machine can access this resource)

        if method in ['POST']:
            validate_str("workflow", param, safe, RX_TASKNAME, optional=True)
            validate_str("status", param, safe, RX_STATUS, optional=True)
            validate_str("command", param, safe, RX_STATUS, optional=True)
            validate_str("getstatus", param, safe, RX_STATUS, optional=True)
            validate_str("failure", param, safe, RX_MANYLINES_SHORT, optional=True)
            validate_strlist("resubmittedjobs", param, safe, RX_JOBID)
            validate_str("workername", param, safe, RX_WORKER_NAME, optional=True)
            validate_str("subresource", param, safe, RX_SUBPOSTWORKER, optional=True)
            validate_num("limit", param, safe, optional=True)
            validate_num("clusterid", param, safe, optional=True) #clusterid of the dag
            # possible combinations to check
            # 1) taskname + status
            # 2) taskname + status + failure
            # 3) taskname + status + resubmitted
            # 4) taskname + status == (1)
            # 5)            status + limit + getstatus + workername
            # 6) taskname + runs + lumis
        elif method in ['GET']:
            validate_str("workername", param, safe, RX_WORKER_NAME, optional=True)
            validate_str("getstatus", param, safe, RX_STATUS, optional=True)
            validate_num("limit", param, safe, optional=True)
            # possible combinations to check
            # 1) workername + getstatus + limit
            # 2) subresource + subjobdef + subuser


    @restcall
    def post(self, workflow, status, command, subresource, failure, resubmittedjobs, getstatus, workername, limit, clusterid):
        """ Updates task information """
        methodmap = {"state": {"args": (self.Task.SetStatusTask_sql,), "method": self.api.modify, "kwargs": {"status": [status],
                     "command": [command], "taskname": [workflow]}},
                     #TODO MM - I don't see where this start API is used
                     "start": {"args": (self.Task.SetReadyTasks_sql,), "method": self.api.modify, "kwargs": {"tm_task_status": [status],
                               "tm_taskname": [workflow]}},
                     "failure": {"args": (self.Task.SetFailedTasks_sql,), "method": self.api.modify, "kwargs": {"tm_task_status": [status],
                                 "failure": [failure], "tm_taskname": [workflow]}},
                  #Used in DagmanSubmitter?
                     "success": {"args": (self.Task.SetInjectedTasks_sql,), "method": self.api.modify, "kwargs": {"tm_task_status": [status],
                                 "tm_taskname": [workflow], "clusterid": [clusterid]}},
                     "process": {"args": (self.Task.UpdateWorker_sql,), "method": self.api.modifynocheck, "kwargs": {"tw_name": [workername],
                                  "get_status": [getstatus], "limit": [limit], "set_status": [status]}},
        }

        if subresource is None:
            subresource = 'state'
        if not subresource in list(methodmap.keys()):
            raise InvalidParameter("Subresource of workflowdb has not been found")
        methodmap[subresource]['method'](*methodmap[subresource]['args'], **methodmap[subresource]['kwargs'])
        return []

    @restcall
    def get(self, workername, getstatus, limit):
        """ Retrieve all columns for a specified task or
            tasks which are in a particular status with
            particular conditions """

        binds = {"limit": limit, "tw_name": workername, "get_status": getstatus}
        rows = self.api.query(None, None, self.Task.GetReadyTasks_sql, **binds)
        for row in rows:
            # get index of tm_user_config to load data from clob and load json string
            newtask = self.Task.GetReadyTasks_tuple(*row)
            yield fixupTask(newtask)


    @restcall
    def delete(self):
        """ Delete a task from the DB """
        raise NotImplementedError


def fixupTask(task):
    """ Fixup some values obtained by the query. """

    result = task._asdict()

    # fixup timestamps
    for field in ['tm_start_time', 'tm_start_injection', 'tm_end_injection']:
        current = result[field]
        result[field] = str(getEpochFromDBTime(current)) if current else ''

    # fixup CLOBS values by calling read (only for Oracle)
    for field in ['tm_task_failure', 'tm_split_args', 'tm_outfiles', 'tm_tfile_outfiles', 'tm_edm_outfiles',
                  'tm_arguments', 'tm_scriptargs', 'tm_user_files', 'tm_user_config']:
        current = result[field]
        fixedCurr = current if (current is None or isinstance(current, str)) else current.read()
        result[field] = fixedCurr

    # fliter_evaluate values
    for field in ['tm_site_whitelist', 'tm_site_blacklist', 'tm_split_args', 'tm_outfiles', 'tm_tfile_outfiles',
                  'tm_edm_outfiles', 'tm_user_infiles', 'tm_arguments', 'tm_scriptargs',
                  'tm_user_files']:
        current = result[field]
        result[field] = literal_eval(current)
        for idx, value in enumerate(result[field]):
            if isinstance(value, bytes):
                result[field][idx] = value.decode("utf8")

    # py3 crabserver compatible with tasks submitted with py2 crabserver
    for arg in ('lumis', 'runs'):
        for idx, val in enumerate(result['tm_split_args'].get(arg)):
            result['tm_split_args'][arg][idx] = decodeBytesToUnicode(val)

    # convert tm_arguments to the desired values
    extraargs = result['tm_arguments']
    result['resubmit_publication'] = extraargs['resubmit_publication'] if 'resubmit_publication' in extraargs else None
    result['resubmit_jobids'] = extraargs['resubmit_jobids'] if 'resubmit_jobids' in extraargs else None
    if result['resubmit_jobids'] is None and 'resubmitList' in extraargs: ## For backward compatibility only.
        result['resubmit_jobids'] = extraargs['resubmitList']
    result['resubmit_site_whitelist'] = extraargs['site_whitelist'] if 'site_whitelist' in extraargs else None
    if result['resubmit_site_whitelist'] is None and 'siteWhiteList' in extraargs: ## For backward compatibility only.
        result['resubmit_site_whitelist'] = extraargs['siteWhiteList']
    result['resubmit_site_blacklist'] = extraargs['site_blacklist'] if 'site_blacklist' in extraargs else None
    if result['resubmit_site_blacklist'] is None and 'siteBlackList' in extraargs: ## For backward compatibility only.
        result['resubmit_site_blacklist'] = extraargs['siteBlackList']
    result['resubmit_maxjobruntime'] = extraargs['maxjobruntime'] if 'maxjobruntime' in extraargs else None
    result['resubmit_maxmemory'] = extraargs['maxmemory'] if 'maxmemory' in extraargs else None
    result['resubmit_numcores'] = extraargs['numcores'] if 'numcores' in extraargs else None
    result['resubmit_priority'] = extraargs['priority'] if 'priority' in extraargs else None

    # load json data of tm_user_config column
    # Hard code default value of tm_user_config for backward compatibility
    # with older task
    if result['tm_user_config']:
        result['tm_user_config'] = json.loads(result['tm_user_config'])
    else:
        result['tm_user_config'] = {'partialdataset': False}
    return result
