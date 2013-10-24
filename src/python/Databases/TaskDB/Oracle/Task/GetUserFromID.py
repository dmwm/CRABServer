#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class GetUserFromID(DBFormatter):
    sql ="SELECT tm_username FROM tasks WHERE tm_taskname=:taskname"

    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn, transaction = transaction)
        return self.format(result)
