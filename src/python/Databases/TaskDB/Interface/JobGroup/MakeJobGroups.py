#!/usr/bin/env python
"""
_MakeJobGroup_
"""
import logging
import Databases.Connection as DBConnect

logger = logging.getLogger(__name__)

def addJobGroup(taskName, jobdefid, status, blocks, jobgroup_failure, tm_user_dn):
    """
    _addJobGroup_
    """
    factory = DBConnect.getConnection(package="Databases.TaskDB")
    newJobGroup = factory(classname = "JobGroup.AddJobGroup")
    try:
#Actually I am not sure I want the following three lines... It can trigger errors later
#Let's discuss this at the ticket review
#        #TODO put 4000 in some variable in Create.py and use it when you create the DB
#        if len(blocks) > 4000:
#            logger.warning("Truncating blocks field as it exceeds 4000 chars")
#            blocks = blocks[:4000]
        jobgroupId = newJobGroup.execute(taskName, jobdefid, status, blocks,
                                         jobgroup_failure, tm_user_dn)
    except Exception, ex:
        msg = "Unable to create jobgroup %s of task named %s\n" % (jobdefid, taskName)
        msg += str(ex)
        raise RuntimeError, msg
    return jobgroupId
