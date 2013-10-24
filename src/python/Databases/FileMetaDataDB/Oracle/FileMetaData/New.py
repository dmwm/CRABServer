#!/usr/bin/env python
"""
_Task.New_
Action to insert a new task into TaskDB
"""
from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = "INSERT INTO filemetadata ("
    sql += "tm_taskname, panda_job_id, fmd_outdataset, fmd_acq_era, fmd_sw_ver, fmd_in_events, fmd_global_tag,\
            fmd_publish_name, fmd_location, fmd_tmp_location, fmd_runlumi, fmd_adler32, fmd_cksum, fmd_md5, fmd_lfn, fmd_size,\
            fmd_type,fmd_parent,fmd_creation_time,fmd_filestate)"
    sql += " VALUES (:taskname, :pandajobid, :outdatasetname, :acquisitionera, :appver, :events, :globalTag,\
                    :publishdataname, :outlocation, :outtmplocation, :runlumi, :checksumadler32, :checksumcksum, :checksummd5, :outlfn, :outsize,\
                    :outtype, :inparentlfns, SYS_EXTRACT_UTC(SYSTIMESTAMP), :filestate)"
