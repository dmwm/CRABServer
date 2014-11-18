alter table tasks add (tm_user_webdir VARCHAR(1000));
alter table tasks add (tm_activity VARCHAR(255));
alter table tasks add (tm_scriptexe VARCHAR(255));
alter table tasks add (tm_scriptargs VARCHAR(4000));
alter table tasks add (tm_extrajdl VARCHAR(1000));
alter table tasks add (tm_generator VARCHAR(255));

# This is needed, because I was not allowed to change from VARCHAR to CLOB.
# And I found this solution 
#tm_outfiles CLOB
ALTER TABLE tasks ADD tm_outfiles1 CLOB;
UPDATE tasks SET tm_outfiles1 = tm_outfiles;
ALTER TABLE tasks DROP column tm_outfiles;
ALTER TABLE tasks RENAME column tm_outfiles1 TO tm_outfiles;

#tm_tfile_outfiles CLOB
ALTER TABLE tasks ADD tm_tfile_outfiles1 CLOB;
UPDATE tasks SET tm_tfile_outfiles1 = tm_tfile_outfiles;
ALTER TABLE tasks DROP column tm_tfile_outfiles;
ALTER TABLE tasks RENAME column tm_tfile_outfiles1 TO tm_tfile_outfiles;

#tm_edm_outfiles CLOB
ALTER TABLE tasks ADD tm_edm_outfiles1 CLOB;
UPDATE tasks SET tm_edm_outfiles1 = tm_edm_outfiles;
ALTER TABLE tasks DROP column tm_edm_outfiles;
ALTER TABLE tasks RENAME column tm_edm_outfiles1 TO tm_edm_outfiles;

ALTER TABLE tasks ADD tm_asourl VARCHAR(4000);
ALTER TABLE tasks ADD tm_events_per_lumi NUMBER(38);
