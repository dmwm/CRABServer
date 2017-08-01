#!/usr/bin/env python
"""
_Databases.FileTransfersDB.Oracle_

Oracle Compatibility layer for File Transfers DB
"""

import threading
from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    """
    Implementation of FileTransfers DB for Oracle
    """
    requiredTables = ['filetransfersdb']

    def __init__(self, logger=None, dbi=None, param=None):
        if dbi == None:
            myThread = threading.currentThread()
            dbi = myThread.dbi
            logger = myThread.logger
        DBCreator.__init__(self, logger, dbi)

        self.create = {}
        self.constraints = {}
        self.create['i_transfers'] = "CREATE INDEX TM_TASKNAME_IDX ON FILETRANSFERSDB (TM_TASKNAME)"
        self.create['i_workers'] = "CREATE INDEX TM_WORKER_STATE ON FILETRANSFERSDB (TM_ASO_WORKER, TM_TRANSFER_STATE) COMPRESS 2"
        #  //
        # // Define create statements for each table
        # //
        #  //
        # tm_id - unique ID which is also the primary key (len 60 chars)
        # tm_user - username (len 30 chars is ENOUGH)
        # tm_workflow - taskName (len 255 chars)
        # tm_group - group which is used inside cert
        # tm_role - role which is used inside cert

        self.create['b_transfers'] = """
        CREATE TABLE filetransfersdb(
        tm_id VARCHAR(60) NOT NULL,
        tm_username VARCHAR(30) NOT NULL,
        tm_taskname VARCHAR(255) NOT NULL,
        tm_destination VARCHAR(100) NOT NULL,
        tm_destination_lfn VARCHAR(1000) NOT NULL,
        tm_source VARCHAR(100) NOT NULL,
        tm_source_lfn VARCHAR(1000) NOT NULL,
        tm_filesize NUMBER(20) NOT NULL,
        tm_publish NUMBER(1) NOT NULL,
        tm_jobid NUMBER(10) NOT NULL,
        tm_job_retry_count NUMBER(5),
        tm_type VARCHAR(20) NOT NULL,
        tm_aso_worker VARCHAR(100),
        tm_transfer_retry_count NUMBER(5) DEFAULT 0,
        tm_transfer_max_retry_count NUMBER(5) DEFAULT 2,
        tm_publication_retry_count NUMBER(5) DEFAULT 0,
        tm_publication_max_retry_count NUMBER(5) DEFAULT 2,
        tm_rest_host VARCHAR(50) NOT NULL,
        tm_rest_uri VARCHAR(255) NOT NULL,
        tm_transfer_state NUMBER(1) NOT NULL,
        tm_publication_state NUMBER(1) NOT NULL,
        tm_transfer_failure_reason VARCHAR(1000),
        tm_publication_failure_reason VARCHAR(1000),
        tm_fts_id VARCHAR(255),
        tm_fts_instance VARCHAR(255),
        tm_last_update NUMBER(11) NOT NULL,
        tm_start_time NUMBER(11) NOT NULL,
        tm_end_time NUMBER(11),
        CONSTRAINT id_pk PRIMARY KEY(tm_id),
        CONSTRAINT fk_tm_taskname_ftdb FOREIGN KEY (tm_taskname) REFERENCES tasks (tm_taskname)
        )
    """

