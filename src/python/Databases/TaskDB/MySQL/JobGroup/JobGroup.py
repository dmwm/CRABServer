#!/usr/bin/env python

import logging

class JobGroup(object):
    """
    """
    AddJobGroup_sql = "INSERT INTO jobgroups ( \
                      tm_taskname, panda_jobdef_id, panda_jobdef_status, \
                      tm_data_blocks, panda_jobgroup_failure, tm_user_dn) \
                      VALUES (%(task_name)s, %(jobdef_id)s, upper(%(jobgroup_status)s), \
                      %(blocks)s, %(jobgroup_failure)s, %(tm_user_dn)s) "
   
   
    GetFailedJobGroup_sql = "SELECT * FROM jobgroups WHERE panda_jobdef_status = 'FAILED'"
   
    GetJobGroupFromID_sql = "SELECT panda_jobdef_id, panda_jobdef_status, \
           	   panda_jobgroup_failure FROM jobgroups WHERE tm_taskname = %(taskname)s"
   
    GetJobGroupFromJobDef_sql = """SELECT tm_taskname, panda_jobdef_id, panda_jobdef_status, \
                       tm_data_blocks, panda_jobgroup_failure, tm_user_dn \
                       FROM jobgroups WHERE panda_jobdef_id = %(jobdef_id)s and \
           	    tm_user_dn = %(user_dn)s"""
   
    SetJobDefId_sql = "UPDATE jobgroups SET panda_jobdef_id = %(panda_jobgroup)s \
           	    WHERE tm_jobgroups_id = %(tm_jobgroups_id)s"
   
    SetStatusJobGroup_sql = "UPDATE jobgroups SET panda_jobdef_status = upper(%(status)s) \
           	          WHERE tm_jobgroups_id = %(tm_jobgroup_id)s"
   
