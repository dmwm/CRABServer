#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class SetStatusJobGroup(DBFormatter):
    sql = "UPDATE jobgroups SET panda_jobdef_status = upper(:status) WHERE tm_jobgroups_id = :tm_jobgroup_id"

    def execute(self, tm_jobgroup, status, conn = None, transaction = False):
        binds = {"tm_jobgroup_id": tm_jobgroup, "status": status}
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.format(result)
