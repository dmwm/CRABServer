#!/usr/bin/env python
"""
_ID_

Given the id of a task (its taskname) get information about it

"""
from WMCore.Database.DBFormatter import DBFormatter


class ID(DBFormatter):
    """
    _ID_

    Get a group ID from the group name

    """

    sql = "SELECT tm_taskname, panda_jobset_id, tm_task_status, tm_user_role, tm_user_group,"
    sql += "tm_task_failure, tm_split_args, panda_resubmitted_jobs, tm_save_logs FROM tasks WHERE tm_taskname=:taskname"

    def execute(self, taskname, conn = None, trans = False):
        """
        _execute_

        Retrieve the ID of the user defined by HN username

        """
        binds = {"taskname": taskname}
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = trans)
        output = self.format(result)
        if len(output) == 0:
            return None
        return output[0][0]


