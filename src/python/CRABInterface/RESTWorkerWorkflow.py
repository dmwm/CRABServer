# WMCore dependecies here
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Validation import validate_str, validate_strlist, validate_num, validate_numlist
from WMCore.REST.Error import InvalidParameter

from ServerUtilities import getEpochFromDBTime
from CRABInterface.Utils import getDBinstance
from CRABInterface.RESTExtensions import authz_login_valid
from CRABInterface.Regexps import RX_TASKNAME, RX_BLOCK, RX_WORKER_NAME, RX_STATUS, RX_TEXT_FAIL, RX_DN, RX_SUBPOSTWORKER, \
                                  RX_SUBGETWORKER, RX_RUNS, RX_LUMIRANGE

# external dependecies here
import cherrypy
import calendar
from ast import literal_eval
from base64 import b64decode


class RESTWorkerWorkflow(RESTEntity):
    """REST entity to handle interactions between CAFTaskWorker and TaskManager database"""

    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        self.Task = getDBinstance(config, 'TaskDB', 'Task')
        self.JobGroup = getDBinstance(config, 'TaskDB', 'JobGroup')

    def validate(self, apiobj, method, api, param, safe):
        """Validating all the input parameter as enforced by the WMCore.REST module"""
        authz_login_valid() #TODO: should we also call authz_operator here ? Otherwise anybody can get tasks from here.
                            #      Actually, maybe something even more strict is necessary (only the prod TW machine can access this resource)

        if method in ['POST']:
            validate_str("workflow", param, safe, RX_TASKNAME, optional=True)
            validate_str("status", param, safe, RX_STATUS, optional=True)
            validate_str("command", param, safe, RX_STATUS, optional=True)
            validate_str("getstatus", param, safe, RX_STATUS, optional=True)
            validate_str("failure", param, safe, RX_TEXT_FAIL, optional=True)
            validate_numlist("resubmittedjobs", param, safe)
            validate_str("workername", param, safe, RX_WORKER_NAME, optional=True)
            validate_str("subresource", param, safe, RX_SUBPOSTWORKER, optional=True)
            validate_num("limit", param, safe, optional=True)
            validate_num("clusterid", param, safe, optional=True) #clusterid of the dag
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
            # possible combinations to check
            # 1) workername + getstatus + limit
            # 2) subresource + subjobdef + subuser


    @restcall
    def post(self, workflow, status, command, subresource, failure, resubmittedjobs, getstatus, workername, limit, clusterid):
        """ Updates task information """
        if failure is not None:
            try:
                failure = b64decode(failure)
            except TypeError:
                raise InvalidParameter("Failure message is not in the accepted format")
        methodmap = {"state": {"args": (self.Task.SetStatusTask_sql,), "method": self.api.modify, "kwargs": {"status": [status],
                                                                               "command": [command], "taskname": [workflow]}},
                   #TODO MM - I don't see where this start API is used
                  "start": {"args": (self.Task.SetReadyTasks_sql,), "method": self.api.modify, "kwargs": {"tm_task_status": [status],
                                                                                       "tm_taskname": [workflow]}},
                  "failure": {"args": (self.Task.SetFailedTasks_sql,), "method": self.api.modify, "kwargs": {"tm_task_status": [status],
                                                                                "failure": [failure],
                                                                               "tm_taskname": [workflow]}},
                  #Used in DagmanSubmitter?
                  "success": {"args": (self.Task.SetInjectedTasks_sql,), "method": self.api.modify, "kwargs": {"tm_task_status": [status],
                                                                                            "tm_taskname": [workflow],
                                                                                            "clusterid": [clusterid],
                                                                                            "resubmitted_jobs": [str(resubmittedjobs)]}},
                  "process": {"args": (self.Task.UpdateWorker_sql,), "method": self.api.modifynocheck, "kwargs": {"tw_name": [workername],
                                                                                                   "get_status": [getstatus],
                                                                                                   "limit": [limit],
                                                                                                   "set_status": [status]}},
        }

        if subresource is None:
            subresource = 'state'
        if not subresource in methodmap.keys():
            raise InvalidParameter("Subresource of workflowdb has not been found")
        methodmap[subresource]['method'](*methodmap[subresource]['args'], **methodmap[subresource]['kwargs'])
        return []

    @restcall
    def get(self, workername, getstatus, limit, subresource):
        """ Retrieve all columns for a specified task or
            tasks which are in a particular status with
            particular conditions """

        binds = {"limit": limit, "tw_name": workername, "get_status": getstatus}
        rows = self.api.query(None, None, self.Task.GetReadyTasks_sql, **binds)
        for row in rows:
            newtask = self.Task.GetReadyTasks_tuple(*row)
            yield fixupTask(newtask)


    @restcall
    def delete(self):
        """ Delete a task from the DB """
        raise NotImplementedError


def fixupTask(task):
    """ Fixup some values obtained by the query. """

    result = task._asdict()

    #fixup timestamps
    for field in ['tm_start_time', 'tm_start_injection', 'tm_end_injection']:
        current = result[field]
        result[field] = str(getEpochFromDBTime(current)) if current else ''

    #fixup CLOBS values by calling read (only for Oracle)
    for field in ['tm_task_failure', 'tm_split_args', 'tm_outfiles', 'tm_tfile_outfiles', 'tm_edm_outfiles',
                  'panda_resubmitted_jobs', 'tm_arguments', 'tm_scriptargs', 'tm_user_files', 'tm_arguments']:
        current = result[field]
        fixedCurr = current if (current is None or isinstance(current, str)) else current.read()
        result[field] = fixedCurr

    #liter_evaluate values
    for field in ['tm_site_whitelist', 'tm_site_blacklist', 'tm_split_args', 'tm_outfiles', 'tm_tfile_outfiles',
                  'tm_edm_outfiles', 'panda_resubmitted_jobs', 'tm_user_infiles', 'tm_arguments', 'tm_scriptargs',
                  'tm_user_files']:
        current = result[field]
        result[field] = literal_eval(current)

    #convert tm_arguments to the desired values
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
    result['kill_ids'] = extraargs['killList'] if 'killList' in extraargs else []
    result['kill_all'] = extraargs['killAll'] if 'killAll' in extraargs else False

    return result
