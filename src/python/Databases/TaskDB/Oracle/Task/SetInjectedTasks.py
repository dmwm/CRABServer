#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class SetInjectedTasks(DBFormatter):
    sql = """UPDATE tasks SET tm_end_injection = SYS_EXTRACT_UTC(SYSTIMESTAMP), tm_task_status = upper(:tm_task_status),
                    panda_jobset_id = :panda_jobset_id, panda_resubmitted_jobs = :resubmitted_jobs
             WHERE tm_taskname = :tm_taskname"""

    def execute(self, tm_taskname, status, jobset_id, resubmitted_jobs, conn = None, transaction = False):
        binds = {"tm_task_status": status, "panda_jobset_id": jobset_id,
                 "tm_taskname": tm_taskname, 'resubmitted_jobs': resubmitted_jobs}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = transaction)
        return self.format(result)
