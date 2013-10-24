#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class GetJobGroupFromJobDef(DBFormatter):
    sql = """SELECT tm_taskname, panda_jobdef_id, panda_jobdef_status, 
                    tm_data_blocks, panda_jobgroup_failure, tm_user_dn
             FROM jobgroups
             WHERE panda_jobdef_id = :jobdef_id and tm_user_dn = :user_dn"""

    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn, transaction = transaction)
        return self.format(result)
