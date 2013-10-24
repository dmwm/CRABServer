#!/usr/bin/env python
"""
_MakeTask_

API for creating a new request in the database

"""
import logging
import Databases.Connection as DBConnect

def createTask(taskName, panda_jobset_id, tm_task_status, tm_task_failure, \
               tm_job_sw, tm_job_arch, tm_input_dataset, tm_site_whitelist, tm_site_blacklist, tm_split_algo, \
               tm_split_args, tm_totalunits, tm_user_sandbox, tm_cache_url, tm_username, tm_user_dn, tm_user_vo, tm_user_role, \
               tm_user_group, tm_publish_name, tm_asyncdest, tm_dbs_url, tm_publish_dbs_url, tm_outfiles, \
               tm_tfile_outfiles, tm_edm_outfiles, tm_transformation, tm_arguments):
    """
    _createTask_

    Create a new task entry, return the task id

    This creates a basic task entity, details of size of task, datasets
    etc need to be added with other API calls

    """
    factory = DBConnect.getConnection(package='Databases.TaskDB')

    #  //
    # // does the task name already exist?
    #//
    taskId = factory(classname = "Task.ID").execute(taskName)
    if taskId != None:
        msg = "task name already exists: %s\n" % taskName
        msg += "Cannot create new task with same name"
        raise RuntimeError, msg

    newTask = factory(classname = "Task.New")
    try:
        taskId = newTask.execute(
            taskName, panda_jobset_id, tm_task_status, tm_task_failure,  \
               tm_job_sw, tm_job_arch, tm_input_dataset, tm_site_whitelist, tm_site_blacklist, tm_split_algo, \
               tm_split_args, tm_totalunits, tm_user_sandbox, tm_cache_url, tm_username, tm_user_dn, tm_user_vo, tm_user_role, \
               tm_user_group, tm_publish_name, tm_asyncdest, tm_dbs_url, tm_publish_dbs_url, tm_outfiles, \
               tm_tfile_outfiles, tm_edm_outfiles, tm_transformation, tm_arguments
            )

    except Exception, ex:
        msg = "Unable to create task named %s\n" % taskName
        msg += str(ex)
        raise RuntimeError, msg
    return taskId
