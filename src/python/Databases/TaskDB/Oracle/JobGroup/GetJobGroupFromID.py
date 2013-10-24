#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class GetJobGroupFromID(DBFormatter):
    sql = "SELECT panda_jobdef_id, panda_jobdef_status, panda_jobgroup_failure FROM jobgroups WHERE tm_taskname = :taskname"

    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn, transaction = transaction)
        return self.format(result)
