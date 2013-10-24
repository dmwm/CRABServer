#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class UpdateWorker(DBFormatter):
    sql = """UPDATE tasks SET tw_name = :tw_name, tm_task_status = :set_status
             WHERE tm_taskname IN (SELECT tm_taskname
                                   FROM (SELECT tm_taskname, rownum as counter
                                         FROM tasks
                                         WHERE tm_task_status = :get_status
                                         ORDER BY tm_start_time)
                                   WHERE counter <= :limit)"""

    def execute(self, getstatus, setstatus, twname, limit, conn = None, transaction = False):
        binds = {"tw_name": twname, "get_status": getstatus,
                 "limit": limit, "set_status": setstatus}
        result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
        return self.format(result)
