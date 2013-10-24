#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class AddJobGroup(DBFormatter):

    sql = "INSERT INTO JOBGROUPS ( "
    sql += "tm_taskname, panda_jobdef_id, panda_jobdef_status, tm_data_blocks, panda_jobgroup_failure, tm_user_dn)"
    sql += " VALUES (:task_name, :jobdef_id, upper(:jobgroup_status), :blocks, :jobgroup_failure, :tm_user_dn) "

    def execute(self, taskName, jobdefid, status, blocks, jobgroup_failure, tm_user_dn,
                conn = None, transaction = False):
        binds = {"task_name": taskName, "jobdef_id": jobdefid, "jobgroup_status": status, "blocks": blocks,
                 "jobgroup_failure": jobgroup_failure, "tm_user_dn": tm_user_dn}
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.format(result)
