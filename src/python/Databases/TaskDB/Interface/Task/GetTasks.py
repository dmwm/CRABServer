#!/usr/bin/env python
"""
_GetTasks_
"""
import logging
import Databases.Connection as DBConnect

def getNewResubmitTasks():
    """
    _getNewResubmitTasks_
    """
    factory = DBConnect.getConnection(package='Databases.TaskDB')
    tasks = factory(classname = "Task.GetNewResubmit")
    try:
        taskList = tasks.execute()
    except Exception, ex:
        msg = "Unable to get new resubmit tasks \n"
        msg += str(ex)
        raise RuntimeError, msg
    return taskList

def getKillTasks():
    """
    _getKillTasks_
    """
    factory = DBConnect.getConnection(package='Databases.TaskDB')
    tasks = factory(classname = "Task.GetKillTasks")
    try:
        taskList = tasks.execute()
    except Exception, ex:
        msg = "Unable to get new resubmit tasks \n"
        msg += str(ex)
        raise RuntimeError, msg
    return taskList

def getReadyTasks(getstatus, twname, limit = 0):
    """
    _getKillTasks_
    """
    factory = DBConnect.getConnection(package='Databases.TaskDB')
    tasks = factory(classname = "Task.GetReadyTasks")
    try:
        taskList = tasks.execute(getstatus, twname, limit)
    except Exception, ex:
        msg = "Unable to get ready tasks \n"
        msg += str(ex)
        raise RuntimeError, msg
    return taskList

def getInjectedTasks():
    """
    _getKillTasks_
    """
    factory = DBConnect.getConnection(package='Databases.TaskDB')
    tasks = factory(classname = "Task.GetInjectedTasks")
    try:
        taskList = tasks.execute()
    except Exception, ex:
        msg = "Unable to get injected tasks \n"
        msg += str(ex)
        raise RuntimeError, msg
    return taskList

def getFailedTasks():
    """
    _getKillTasks_
    """
    factory = DBConnect.getConnection(package='Databases.TaskDB')
    tasks = factory(classname = "Task.GetFailedTasks")
    try:
        taskList = tasks.execute()
    except Exception, ex:
        msg = "Unable to get failed tasks \n"
        msg += str(ex)
        raise RuntimeError, msg
    return taskList
