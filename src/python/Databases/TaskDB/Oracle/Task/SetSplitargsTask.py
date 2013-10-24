#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class SetSplitargsTask(DBFormatter):
    sql = "UPDATE tasks SET tm_split_args = :splitargs WHERE tm_taskname = :taskname"

    def execute(self, taskName, status, conn = None, transaction = False):
        binds = {"taskname": taskName, "splitargs": arguments}
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)

        return self.format(result)
