#!/usr/bin/env python
"""
_Databases.TaskDB.MySQL_

MySQL Compatibility layer for Task Manager DB
"""

import threading
from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    """
    Implementation of TaskMgr DB for MySQL
    """
    requiredTables = ['tasks',
                      'jobgroups'
                      ]

    def __init__(self, logger=None, dbi=None, param=None):
        if dbi == None:
            myThread = threading.currentThread()
            dbi = myThread.dbi
            logger = myThread.logger
        DBCreator.__init__(self, logger, dbi)

        self.create = {}
        self.constraints = {}
        #  //
        # // Define create statements for each table
        #//
        #  //
        self.create['b_tasks'] = """
        CREATE TABLE tasks(
        tm_taskname VARCHAR(255) NOT NULL,
        tm_activity VARCHAR(255),
        panda_jobset_id BIGINT,
        tm_task_status VARCHAR(255) NOT NULL,
        tm_start_time TIMESTAMP,
        tm_start_injection TIMESTAMP,
        tm_end_injection TIMESTAMP,
        tm_task_failure LONGTEXT,
        tm_job_sw VARCHAR(255) NOT NULL,
        tm_job_arch VARCHAR(255),
        tm_input_dataset VARCHAR(500),
        tm_use_parent BIGINT,
        tm_site_whitelist VARCHAR(4000),
        tm_site_blacklist VARCHAR(4000),
        tm_split_algo VARCHAR(255) NOT NULL,
        tm_split_args LONGTEXT NOT NULL,
        tm_totalunits BIGINT,
        tm_user_sandbox VARCHAR(255) NOT NULL,
        tm_cache_url VARCHAR(255) NOT NULL,
        tm_username VARCHAR(255) NOT NULL,
        tm_user_dn VARCHAR(255) NOT NULL,
        tm_user_vo VARCHAR(255) NOT NULL,
        tm_user_role VARCHAR(255),
        tm_user_group VARCHAR(255),
        tm_publish_name VARCHAR(500),
        tm_asyncdest VARCHAR(255) NOT NULL,
        tm_dbs_url VARCHAR(255) NOT NULL,
        tm_publish_dbs_url VARCHAR(255),
        tm_publication VARCHAR(1) NOT NULL,
        tm_outfiles LONGTEXT,
        tm_tfile_outfiles LONGTEXT,
        tm_edm_outfiles LONGTEXT,
        tm_job_type VARCHAR(255) NOT NULL,
        tm_arguments LONGTEXT,
        panda_resubmitted_jobs LONGTEXT,
        tm_save_logs VARCHAR(1) NOT NULL,
        tw_name VARCHAR(255),
        tm_user_infiles VARCHAR(4000),
        tm_maxjobruntime BIGINT,
        tm_numcores BIGINT,
        tm_maxmemory BIGINT,
        tm_priority BIGINT,
        tm_output_dataset LONGTEXT,
        tm_task_warnings LONGTEXT DEFAULT '[]',
        tm_user_webdir VARCHAR(1000),
        tm_scriptexe VARCHAR(255),
        tm_scriptargs VARCHAR(4000),
        tm_extrajdl VARCHAR(1000),
        tm_asourl VARCHAR(4000),
        tm_collector VARCHAR(1000),
        tm_schedd VARCHAR(255),
        tm_dry_run VARCHAR(1),
        tm_user_files LONGTEXT DEFAULT '[]',
        tm_transfer_outputs VARCHAR(1),
        tm_output_lfn VARCHAR(1000),
        tm_ignore_locality VARCHAR(1),
        tm_fail_limit BIGINT,
        tm_one_event_mode VARCHAR(1),
        CONSTRAINT taskname_pk PRIMARY KEY(tm_taskname),
        CONSTRAINT check_tm_publication CHECK (tm_publication IN ('T', 'F')),
        CONSTRAINT check_tm_dry_run CHECK (tm_dry_run IN ('T', 'F')),
        CONSTRAINT check_tm_save_logs CHECK (tm_save_logs IN ('T', 'F')),
        CONSTRAINT check_tm_transfer_outputs CHECK (tm_transfer_outputs IN ('T', 'F')),
        CONSTRAINT check_tm_ignore_locality CHECK (tm_ignore_locality IN ('T', 'F')),
        CONSTRAINT check_tm_one_event_mode CHECK (tm_one_event_mode IN ('T', 'F'))
        ) ENGINE=InnoDB 
        """
        self.create['c_jobgroups'] = """
        CREATE TABLE jobgroups(
        tm_jobgroups_id BIGINT NOT NULL AUTO_INCREMENT,
        tm_taskname VARCHAR(255) NOT NULL,
        panda_jobdef_id BIGINT,
        panda_jobdef_status VARCHAR(255) NOT NULL,
        tm_data_blocks LONGTEXT,
        panda_jobgroup_failure LONGTEXT,
        tm_user_dn VARCHAR(255) NOT NULL,
        CONSTRAINT taskname_fk FOREIGN KEY(tm_taskname) references
            tasks(tm_taskname)
            ON DELETE CASCADE,
        CONSTRAINT jobgroup_id_pk PRIMARY KEY(tm_jobgroups_id)
        )  ENGINE=InnoDB
        """
