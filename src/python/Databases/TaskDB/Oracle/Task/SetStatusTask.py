#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class SetStatusTask(DBFormatter):
    sql = "UPDATE tasks SET tm_task_status = upper(:status) WHERE tm_taskname = :taskname"

    def execute(self, taskName, status, conn = None, transaction = False):
        binds = {"taskname": taskName, "status": status}
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)

        return self.format(result)
