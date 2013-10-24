#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class SetReadyTasks(DBFormatter):
    sql = "UPDATE tasks SET tm_start_injection = SYS_EXTRACT_UTC(SYSTIMESTAMP), tm_task_status = upper(:tm_task_status)  WHERE tm_taskname = :tm_taskname"

    def execute(self, tm_taskname, status, conn = None, transaction = False):
        binds = {"tm_task_status": status, "tm_taskname": tm_taskname}
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.format(result)
