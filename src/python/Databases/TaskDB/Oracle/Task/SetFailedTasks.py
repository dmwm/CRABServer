#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class SetFailedTasks(DBFormatter):
    sql = "UPDATE tasks SET tm_end_injection = SYS_EXTRACT_UTC(SYSTIMESTAMP), tm_task_status = UPPER(:tm_task_status), tm_task_failure = :failure WHERE tm_taskname = :tm_taskname"

    def execute(self, tm_taskname, status, failure, conn = None, transaction = False):
        binds = {"tm_task_status": status, "failure": failure, "tm_taskname": tm_taskname}
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.format(result)
