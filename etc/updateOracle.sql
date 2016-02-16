alter table tasks add (tm_user_webdir VARCHAR(1000));
alter table tasks add (tm_activity VARCHAR(255));
alter table tasks add (tm_scriptexe VARCHAR(255));
alter table tasks add (tm_scriptargs VARCHAR(4000));
alter table tasks add (tm_extrajdl VARCHAR(1000));
alter table tasks add (tm_generator VARCHAR(255));

-- This is needed, because I was not allowed to change from VARCHAR to CLOB.
-- And I found this solution
--tm_outfiles CLOB
ALTER TABLE tasks ADD tm_outfiles1 CLOB;
UPDATE tasks SET tm_outfiles1 = tm_outfiles;
ALTER TABLE tasks DROP column tm_outfiles;
ALTER TABLE tasks RENAME column tm_outfiles1 TO tm_outfiles;

--tm_tfile_outfiles CLOB
ALTER TABLE tasks ADD tm_tfile_outfiles1 CLOB;
UPDATE tasks SET tm_tfile_outfiles1 = tm_tfile_outfiles;
ALTER TABLE tasks DROP column tm_tfile_outfiles;
ALTER TABLE tasks RENAME column tm_tfile_outfiles1 TO tm_tfile_outfiles;

--tm_edm_outfiles CLOB
ALTER TABLE tasks ADD tm_edm_outfiles1 CLOB;
UPDATE tasks SET tm_edm_outfiles1 = tm_edm_outfiles;
ALTER TABLE tasks DROP column tm_edm_outfiles;
ALTER TABLE tasks RENAME column tm_edm_outfiles1 TO tm_edm_outfiles;

ALTER TABLE tasks ADD tm_asourl VARCHAR(4000);

--Changes that will be needed for version 3.3.12
ALTER TABLE tasks ADD tm_events_per_lumi NUMBER(38); --for issue #4499
alter table tasks add (tm_use_parent NUMBER(1));
alter table filemetadata add (fmd_tmplfn VARCHAR(500));
update tasks set tm_task_warnings = '[]';
alter table tasks modify(tm_task_warnings DEFAULT '[]');
ALTER TABLE tasks ADD(tm_output_dataset CLOB);
UPDATE filemetadata SET fmd_tmplfn=fmd_lfn WHERE fmd_tmplfn is NULL or fmd_tmplfn='';

--Changes that will be needed for version 3.3.13
alter table tasks add (tm_collector VARCHAR(1000));
ALTER TABLE tasks DROP column tm_transformation;

--Changes that will be needed for version 3.3.14
ALTER TABLE tasks ADD (tm_schedd VARCHAR(255));

--Changes that will be needed for version 3.3.15
alter table tasks add (tm_dry_run VARCHAR(1)); --for issue ##4462

--Changes that will be needed for version 3.3.16
alter table tasks add (tm_user_files CLOB DEFAULT '[]');
alter table tasks add (tm_transfer_outputs VARCHAR(1));
alter table tasks add (tm_output_lfn VARCHAR(1000));
alter table tasks add (tm_ignore_locality VARCHAR(1));
alter table tasks add (tm_fail_limit NUMBER(38));
alter table tasks add (tm_one_event_mode VARCHAR(1));

--Changes that will be needed for version 3.3.1507
alter table tasks add (tm_publish_groupname VARCHAR(1) DEFAULT 'F');
alter table tasks add constraint check_tm_publish_groupname check (tm_publish_groupname IN ('T', 'F')) ENABLE;
alter table tasks add (tm_nonvalid_input_dataset VARCHAR(1) DEFAULT 'T');
alter table tasks add constraint ck_tm_nonvalid_input_dataset check (tm_nonvalid_input_dataset IN ('T', 'F')) ENABLE;

--Changes that will be needed for version 3.3.1509
alter table tasks add (tm_secondary_input_dataset VARCHAR(500));

--Changes that will be needed for version 3.3.1511
alter table tasks add (tm_primary_dataset VARCHAR(255));

--Changes that will be needed for version 3.3.1602
alter table tasks add (tm_asodb VARCHAR(20));

--Changes that will be needed for version 3.3.1603
alter table tasks add (tm_task_command VARCHAR(20));