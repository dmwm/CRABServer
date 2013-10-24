#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class GetFailedJobGroup(DBFormatter):
    sql = "SELECT * FROM jobgroups WHERE panda_jobdef_status = 'FAILED'"

    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn, transaction = transaction)
        return self.format(result)
