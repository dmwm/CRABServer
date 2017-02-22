#!/usr/bin/env python
"""
_Databases.FileMetaDataDB.Oracle_

Oracle Compatibility layer for Task Manager DB
"""

import threading
from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    """
    Implementation of TaskMgr DB for Oracle
    """
    requiredTables = ['filemetadata']

    def __init__(self, logger=None, dbi=None, param=None):
        if dbi == None:
            myThread = threading.currentThread()
            dbi = myThread.dbi
            logger = myThread.logger
        DBCreator.__init__(self, logger, dbi)

        self.create = {}
        self.constraints = {}
        # Define create statements for each table
        self.create['b_filemetadata'] = """
            CREATE TABLE filemetadata (
              tm_taskname VARCHAR(255) NOT NULL,
              panda_job_id NUMBER(11),
              job_id VARCHAR(20),
              fmd_outdataset VARCHAR(500) NOT NULL,
              fmd_acq_era VARCHAR(255) NOT NULL,
              fmd_sw_ver VARCHAR(255) NOT NULL,
              fmd_in_events NUMBER(11) DEFAULT 0,
              fmd_global_tag VARCHAR(255) DEFAULT NULL,
              fmd_publish_name VARCHAR(255) NOT NULL,
              fmd_location VARCHAR(255) NOT NULL,
              fmd_tmp_location VARCHAR(255) NOT NULL,
              fmd_runlumi CLOB,
              fmd_adler32 VARCHAR(10) DEFAULT NULL,
              fmd_cksum NUMBER(11) DEFAULT NULL,
              fmd_md5 VARCHAR(50) DEFAULT NULL,
              fmd_lfn VARCHAR(500) NOT NULL,
              fmd_size NUMBER(11) NOT NULL,
              fmd_type VARCHAR(50) NOT NULL,
              fmd_parent CLOB,
              fmd_creation_time TIMESTAMP NOT NULL,
              fmd_filestate VARCHAR(20),
              fmd_direct_stageout VARCHAR(1),
              fmd_tmplfn VARCHAR(500),
              CONSTRAINT pk_tasklfn PRIMARY KEY(tm_taskname, fmd_lfn),
              CONSTRAINT fk_tm_taskname FOREIGN KEY (tm_taskname) REFERENCES tasks (tm_taskname)
            )
        """
