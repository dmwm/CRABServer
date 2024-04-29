-- Comment out old lines wich are by now absorbed in DB creation scripts.
-- keep the idea of having this script around to push new changes, but avoid
-- to repeat done things all of the times
-- also it is a useful repository of tricks needed for modifying schema.
-- ===============================================================================
-- alter table tasks add (tm_user_webdir VARCHAR(1000));
-- alter table tasks add (tm_activity VARCHAR(255));
-- alter table tasks add (tm_scriptexe VARCHAR(255));
-- alter table tasks add (tm_scriptargs VARCHAR(4000));
-- alter table tasks add (tm_extrajdl VARCHAR(1000));
-- alter table tasks add (tm_generator VARCHAR(255));

-- This is needed, because I was not allowed to change from VARCHAR to CLOB.
-- And I found this solution
--tm_outfiles CLOB
-- ALTER TABLE tasks ADD tm_outfiles1 CLOB;
-- UPDATE tasks SET tm_outfiles1 = tm_outfiles;
-- ALTER TABLE tasks DROP column tm_outfiles;
-- ALTER TABLE tasks RENAME column tm_outfiles1 TO tm_outfiles;

--tm_tfile_outfiles CLOB
-- ALTER TABLE tasks ADD tm_tfile_outfiles1 CLOB;
-- UPDATE tasks SET tm_tfile_outfiles1 = tm_tfile_outfiles;
-- ALTER TABLE tasks DROP column tm_tfile_outfiles;
-- ALTER TABLE tasks RENAME column tm_tfile_outfiles1 TO tm_tfile_outfiles;

--tm_edm_outfiles CLOB
-- ALTER TABLE tasks ADD tm_edm_outfiles1 CLOB;
-- UPDATE tasks SET tm_edm_outfiles1 = tm_edm_outfiles;
-- ALTER TABLE tasks DROP column tm_edm_outfiles;
-- ALTER TABLE tasks RENAME column tm_edm_outfiles1 TO tm_edm_outfiles;

-- ALTER TABLE tasks ADD tm_asourl VARCHAR(4000);

--Changes that will be needed for version 3.3.12
-- ALTER TABLE tasks ADD tm_events_per_lumi NUMBER(38); --for issue #4499
-- alter table tasks add (tm_use_parent NUMBER(1));
-- alter table filemetadata add (fmd_tmplfn VARCHAR(500));
-- update tasks set tm_task_warnings = '[]';
-- alter table tasks modify(tm_task_warnings DEFAULT '[]');
-- ALTER TABLE tasks ADD(tm_output_dataset CLOB);
-- UPDATE filemetadata SET fmd_tmplfn=fmd_lfn WHERE fmd_tmplfn is NULL or fmd_tmplfn='';

--Changes that will be needed for version 3.3.13
-- alter table tasks add (tm_collector VARCHAR(1000));
-- ALTER TABLE tasks DROP column tm_transformation;

--Changes that will be needed for version 3.3.14
-- ALTER TABLE tasks ADD (tm_schedd VARCHAR(255));

--Changes that will be needed for version 3.3.15
-- alter table tasks add (tm_dry_run VARCHAR(1)); --for issue ##4462

--Changes that will be needed for version 3.3.16
-- alter table tasks add (tm_user_files CLOB DEFAULT '[]');
-- alter table tasks add (tm_transfer_outputs VARCHAR(1));
-- alter table tasks add (tm_output_lfn VARCHAR(1000));
-- alter table tasks add (tm_ignore_locality VARCHAR(1));
-- alter table tasks add (tm_fail_limit NUMBER(38));
-- alter table tasks add (tm_one_event_mode VARCHAR(1));

--Changes that will be needed for version 3.3.1507
-- alter table tasks add (tm_publish_groupname VARCHAR(1) DEFAULT 'F');
-- alter table tasks add constraint check_tm_publish_groupname check (tm_publish_groupname IN ('T', 'F')) ENABLE;
-- alter table tasks add (tm_nonvalid_input_dataset VARCHAR(1) DEFAULT 'T');
-- alter table tasks add constraint ck_tm_nonvalid_input_dataset check (tm_nonvalid_input_dataset IN ('T', 'F')) ENABLE;

--Changes that will be needed for version 3.3.1509
-- alter table tasks add (tm_secondary_input_dataset VARCHAR(500));

--Changes that will be needed for version 3.3.1511
-- alter table tasks add (tm_primary_dataset VARCHAR(255));

--Changes that will be needed for version 3.3.1602
-- alter table tasks add (tm_asodb VARCHAR(20));

--Changes that will be needed for version 3.3.1604
-- alter table tasks add (tm_task_command VARCHAR(20));
--THE FOLLOWING UPDATES NECESSARILY NEEDS TO TUN ONCE THE UPDATE IS COMPELTE ON CMSWEB AND BEFORE RESTARTING THE TW
-- UPDATE tasks SET tm_task_status='FAILED' where tm_task_status='QUEUED'; --that's the usual command to fail locked QUEUED task
-- UPDATE tasks SET tm_task_status='FAILED' where tm_task_status='HOLDING'; --in addition we need to do this for holding tasks
--these are command to take care of tasks submitted to the old server and not processed by the TW
-- UPDATE tasks SET tm_task_status='NEW',tm_task_command='KILL' WHERE tm_task_status='KILL';
-- UPDATE tasks SET tm_task_status='NEW',tm_task_command='RESUBMIT' WHERE tm_task_status='RESUBMIT';
-- UPDATE tasks SET tm_task_status='NEW',tm_task_command='SUBMIT' WHERE tm_task_status='NEW';

--Changes that will be needed for version 3.3.1605
-- alter table tasks add (clusterid NUMBER(10));

--Changes that will be needed for version 3.3.1607
-- alter table tasks add (tm_debug_files VARCHAR(255));

--Changse that will be needed for version 3.3.1611
-- alter table tasks add (tm_submitter_ip_addr VARCHAR(45));
-- alter table tasks add (tm_ignore_global_blacklist VARCHAR(1));

--Changse that will be needed for version 3.3.1612

--Changes for the Autmatic Splitting (or resplitting).
-- alter table filemetadata add (job_id VARCHAR(20));
-- alter table filemetadata modify (panda_job_id null);

--Other changes
-- alter table tasks add (tm_last_publication TIMESTAMP);

--change the type of the column tm_jobid
-- ALTER TABLE filetransfersdb ADD tm_jobid_varchar VARCHAR(20);
-- UPDATE filetransfersdb SET tm_jobid_varchar = tm_jobid;
-- ALTER TABLE filetransfersdb DROP COLUMN tm_jobid;
-- ALTER TABLE filetransfersdb RENAME COLUMN tm_jobid_varchar TO tm_jobid;

--Add the column to be used for the filetrasfersdb partitioning
-- ALTER TABLE filetransfersdb ADD tm_creation_time TIMESTAMP;

--Change number of totalUnits to allow fractions
-- ALTER TABLE tasks ADD tm_totalunits_fraction NUMBER(38,6);
-- UPDATE tasks SET tm_totalunits_fraction = tm_totalunits;
-- ALTER TABLE tasks DROP COLUMN tm_totalunits;
-- ALTER TABLE tasks RENAME COLUMN tm_totalunits_fraction TO tm_totalunits;

--Add the column to store the DDM request ID
-- ALTER TABLE tasks modify tm_DDM_reqid VARCHAR(32);

--Add new column to store generic user config as json kv
-- ALTER TABLE tasks ADD tm_user_config CLOB;

--allow NULL in soon-to-be-removed columns
alter table filetransfersdb modify TM_REST_URI varchar(1000) NULL;
alter table filetransfersdb modify TM_REST_HOST varchar(1000) NULL;

-- Add new columns to allow RUCIO_Transfers communications with Publisher
ALTER TABLE filetransfersdb ADD tm_dbs_blockname VARCHAR(1000);
ALTER TABLE filetransfersdb ADD tm_block_complete VARCHAR(10);

--Add Rucio ASO's transfer container name and rule id)
ALTER TABLE tasks ADD tm_transfer_container VARCHAR(1000);
ALTER TABLE tasks ADD tm_transfer_rule VARCHAR(255);
ALTER TABLE tasks ADD tm_publish_rule VARCHAR(255);

--Add Rucio ASO's json kv for store container names and its rules.
ALTER TABLE tasks ADD tm_multipub_rule CLOB;
