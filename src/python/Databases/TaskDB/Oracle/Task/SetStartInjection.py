#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class SetStartInjection(DBFormatter):
    sql = "UPDATE tasks SET tm_start_injection = SYS_EXTRACT_UTC(SYSTIMESTAMP)  WHERE tm_taskname = :tm_taskname"

    def execute(self, tm_taskname, conn = None, transaction = False):
        binds = {"tm_taskname": tm_taskname}
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.format(result)
