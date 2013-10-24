"""
_Destroy_

"""
import threading
import string

from WMCore.Database.DBCreator import DBCreator
from Databases.FileMetaDataDB.Oracle.Create import Create

class Destroy(DBCreator):
    def __init__(self, logger = None, dbi = None, param=None):
        """
        _init_

        Call the base class's constructor and add all necessary tables for
        deletion
        """
        myThread = threading.currentThread()

        if logger == None:
            logger = myThread.logger
        if dbi == None:
            dbi = myThread.dbi

        DBCreator.__init__(self, logger, dbi)
        orderedTables = Create.requiredTables[:]
        orderedTables.reverse()
        i = 0
        for tableName in orderedTables:
            i += 1
            prefix = string.zfill(i, 2)
            self.create[prefix + tableName] = "DROP TABLE %s" % tableName

