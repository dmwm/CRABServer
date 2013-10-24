#!/usr/bin/env python
"""
_GetJobGroups_
"""
import logging
import Databases.Connection as DBConnect

def getFailedGroups():
    """
    _getFailedGroups_
    """
    factory = DBConnect.getConnection(package='Databases.TaskDB')
    failedJobGroup = factory(classname = "JobGroup.GetFailedJobGroup")
    try:
        failed_jobgroup = failedJobGroup.execute()
    except Exception, ex:
        msg = "Unable to get failed jobgroups \n"
        msg += str(ex)
        raise RuntimeError, msg
    return failed_jobgroup
