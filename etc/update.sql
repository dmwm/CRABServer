ALTER TABLE TASKS ADD (tm_user_infiles VARCHAR(255));

--Add fmd_filestate column. #4047
ALTER TABLE filemetadata ADD (fmd_filestate VARCHAR(20));
