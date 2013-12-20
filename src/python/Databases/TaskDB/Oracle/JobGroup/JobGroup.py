#!/usr/bin/env python
"""
"""

class JobGroup(object):

    sql = "INSERT INTO JOBGROUPS ( "
    sql += "tm_taskname, panda_jobdef_id, panda_jobdef_status, tm_data_blocks, panda_jobgroup_failure, tm_user_dn)"
    sql += " VALUES (:task_name, :jobdef_id, upper(:jobgroup_status), :blocks, :jobgroup_failure, :tm_user_dn) "

    AddJobGroup_sql = sql

    GetFailedJobGroup_sql = "SELECT * FROM jobgroups WHERE panda_jobdef_status = 'FAILED'"

    GetJobGroupFromID_sql = "SELECT panda_jobdef_id, panda_jobdef_status, panda_jobgroup_failure FROM jobgroups WHERE tm_taskname = :taskname"

    GetJobGroupFromJobDef_sql = """SELECT tm_taskname, panda_jobdef_id, panda_jobdef_status, 
                    tm_data_blocks, panda_jobgroup_failure, tm_user_dn
             FROM jobgroups
             WHERE panda_jobdef_id = :jobdef_id and tm_user_dn = :user_dn"""

    SetJobDefId_sql = "UPDATE jobgroups SET panda_jobdef_id = :panda_jobgroup  WHERE tm_jobgroups_id = :tm_jobgroups_id"

    SetStatusJobGroup_sql = "UPDATE jobgroups SET panda_jobdef_status = upper(:status) WHERE tm_jobgroups_id = :tm_jobgroup_id"

