#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class SetJobDefId(DBFormatter):
    sql = "UPDATE jobgroups SET panda_jobdef_id = :panda_jobgroup  WHERE tm_jobgroups_id = :tm_jobgroups_id"

    def execute(self, tm_jobgroup, panda_jobgroup, conn = None, transaction = False):
        binds = {"tm_jobgroups_id": tm_jobgroup, "panda_jobgroup": panda_jobgroup}
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.format(result)
