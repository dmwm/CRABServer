#!/usr/bin/env python
"""
_Task.New_
Action to insert a new task into TaskDB
"""
from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    """
    """
    sql = "INSERT INTO tasks ( "
    sql += "tm_taskname,panda_jobset_id, tm_task_status, tm_start_time, tm_task_failure, tm_job_sw, \
            tm_job_arch, tm_input_dataset, tm_site_whitelist, tm_site_blacklist, \
            tm_split_algo, tm_split_args, tm_totalunits, tm_user_sandbox, tm_cache_url, tm_username, tm_user_dn, \
            tm_user_vo, tm_user_role, tm_user_group, tm_publish_name, tm_asyncdest, tm_dbs_url, tm_publish_dbs_url, \
            tm_publication, tm_outfiles, tm_tfile_outfiles, tm_edm_outfiles, tm_transformation, tm_job_type, tm_arguments,\
            panda_resubmitted_jobs, tm_save_logs, tm_user_infiles)"
    sql += " VALUES (:task_name, :jobset_id, upper(:task_status), SYS_EXTRACT_UTC(SYSTIMESTAMP), :task_failure, :job_sw, \
            :job_arch, :input_dataset, :site_whitelist, :site_blacklist, :split_algo, :split_args, :total_units, :user_sandbox, \
            :cache_url, :username, :user_dn, \
            :user_vo, :user_role, :user_group, :publish_name, :asyncdest, :dbs_url, :publish_dbs_url, \
            :publication, :outfiles, :tfile_outfiles, :edm_outfiles, :transformation, :job_type, :arguments,\
            :resubmitted_jobs, :save_logs, :user_infiles)"

    def execute(self, taskName, jobsetId, taskStatus, taskFailure, jobSw, jobArch, inputDataset, \
                siteWhitelist, siteBlacklist, splitAlgo, splitArgs, totalUnits, userSandbox, cacheUrl, username, userDn, \
                userVo, userRole, userGroup, publishName, asyncDest, dbsUrl, publishDbsUrl, publication, outFiles, tfileOutfiles, \
                edmOutfiles, transformation, jobtype, arguments, resubmittedJobs, saveLogs, userInFiles, \
                conn = None, transaction = False):
        """
        """
        binds = {"task_name": taskName, "jobset_id": jobsetId, "task_status": taskStatus, "task_failure": taskFailure, \
                 "job_sw": jobSw, "job_arch": jobArch, "input_dataset": inputDataset, "site_whitelist": siteWhitelist, \
                 "site_blacklist": siteBlacklist, "split_algo": splitAlgo, "split_args": splitArgs, "total_units": totalUnits, "user_sandbox": userSandbox, \
                 "cache_url": cacheUrl, "username": username, "user_dn": userDn, \
                 "user_vo": userVo, "user_role": userRole, "user_group": userGroup, "publish_name": publishName, \
                 "asyncdest": asyncDest, "dbs_url": dbsUrl, "publish_dbs_url": publishDbsUrl, "publication": publication, \
                 "outfiles": outFiles, "tfile_outfiles": tfileOutfiles, "edm_outfiles": edmOutfiles, \
                 "transformation": transformation, "job_type": jobtype, "arguments": arguments, "resubmitted_jobs": resubmittedJobs, \
                 "save_logs": saveLogs, "user_infiles": userInFiles}

        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.format(result)
