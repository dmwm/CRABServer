#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class GetReadyTasks(DBFormatter):
    """
    """
    sql = """SELECT tm_taskname, panda_jobset_id, tm_task_status, \
                    tm_start_time, tm_start_injection, tm_end_injection, \
                    tm_task_failure, tm_job_sw, tm_job_arch, tm_input_dataset, \
                    tm_site_whitelist, tm_site_blacklist, tm_split_algo, tm_split_args, \
                    tm_totalunits, tm_user_sandbox, tm_cache_url, tm_username, tm_user_dn, tm_user_vo, \
                    tm_user_role, tm_user_group, tm_publish_name, tm_asyncdest, tm_dbs_url, \
                    tm_publish_dbs_url, tm_publication, tm_outfiles, tm_tfile_outfiles, tm_edm_outfiles, \
                    tm_transformation, tm_job_type, tm_arguments, panda_resubmitted_jobs, tm_save_logs, \
                    tm_user_infiles, tw_name
                    FROM tasks
                    WHERE tm_task_status = :get_status
                    AND ROWNUM <= :limit AND tw_name = :tw_name"""

    def execute(self, getstatus, twname, limit, conn = None, transaction = False):
        binds = {"limit": limit, "tw_name": twname, "get_status":getstatus}
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                          transaction = transaction)
        return self.format(result)
