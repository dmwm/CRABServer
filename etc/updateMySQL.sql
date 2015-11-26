--https://github.com/dmwm/CRABServer/issues/4154
ALTER TABLE TASKS ADD (tm_maxjobruntime BIGINT, tm_numcores BIGINT, tm_maxmemory BIGINT, tm_priority BIGINT);
