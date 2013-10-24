#!/usr/bin/env python
"""
_SetTasks_
"""
import logging
import Databases.Connection as DBConnect

def setStatus(tm_jobgroups_id, status):
    """
    _setQueuedTask_
    """
    factory = DBConnect.getConnection(package="Databases.TaskDB")
    statusJobGroup = factory(classname = "JobGroup.SetStatusJobGroup")
    try:
        statusJobGroup.execute(tm_jobgroups_id, status)
    except Exception, ex:
        msg = "Unable to get new resubmit tasks \n"
        msg += str(ex)
        raise RuntimeError, msg
    return

def setJobDefId(tm_jobgroups_id, panda_jobdef_id):
    """
    _setJobDefId_
    """
    factory = DBConnect.getConnection()
    jobDefId = factory(classname = "JobGroup.SetJobDefId")
    try:
        jobDefId.execute(tm_jobgroups_id, panda_jobdef_id)
    except Exception, ex:
        msg = "Unable to set the jobdefid \n"
        msg += str(ex)
        raise RuntimeError, msg
    return
