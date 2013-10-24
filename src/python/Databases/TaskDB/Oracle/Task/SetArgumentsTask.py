#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class SetArgumentsTask(DBFormatter):
    sql = "UPDATE tasks SET tm_arguments = :arguments WHERE tm_taskname = :taskname"

    def execute(self, taskName, status, conn = None, transaction = False):
        binds = {"taskname": taskName, "arguments": arguments}
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)

        return self.format(result)
