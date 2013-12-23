--https://github.com/dmwm/CRABServer/issues/4154
ALTER TABLE TASKS ADD (tm_maxjobruntime NUMBER(38), tm_numcores NUMBER(38), tm_maxmemory NUMBER(38), tm_priority NUMBER(38));
