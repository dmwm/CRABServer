#!/usr/bin/env python

import logging

class GetFromTaskAndType():
    """ Used for indexing columns retrieved by the GetFromTaskAndType_sql query
    """
    PANDAID, OUTDS, ACQERA, SWVER, INEVENTS, GLOBALTAG, PUBLISHNAME, LOCATION, TMPLOCATION, RUNLUMI, ADLER32, CKSUM, MD5, LFN, SIZE, PARENTS, STATE = range(17)

class FileMetaData(object):
    """
    """

    ChangeFileState_sql = """UPDATE filemetadata SET fmd_filestate=:filestate \
                             WHERE fmd_lfn=:outlfn and tm_taskname=:taskname """

    GetFromTaskAndType_sql = """SELECT panda_job_id AS pandajobid, \
                           fmd_outdataset AS outdataset, \
                           fmd_acq_era AS acquisitionera, \
                           fmd_sw_ver AS swversion, \
                           fmd_in_events AS inevents, \
                           fmd_global_tag AS globaltag, \
                           fmd_publish_name AS publishname, \
                           fmd_location AS location, \
                           fmd_tmp_location AS tmplocation, \
                           fmd_runlumi AS runlumi, \
                           fmd_adler32 AS adler32, \
                           fmd_cksum AS cksum, \
                           fmd_md5 AS md5, \
                           fmd_lfn AS lfn, \
                           fmd_size AS filesize, \
                           fmd_parent AS parents, \
                           fmd_filestate AS state, \
                           fmd_creation_time AS created \
                    FROM filemetadata \
                    WHERE tm_taskname = :taskname \
                    AND fmd_type IN (SELECT REGEXP_SUBSTR(:filetype, '[^,]+', 1, LEVEL) FROM DUAL CONNECT BY LEVEL <= REGEXP_COUNT(:filetype, ',') + 1) \
                    ORDER BY fmd_creation_time DESC
             """

    New_sql = "INSERT INTO filemetadata ( \
               tm_taskname, panda_job_id, fmd_outdataset, fmd_acq_era, fmd_sw_ver, fmd_in_events, fmd_global_tag,\
               fmd_publish_name, fmd_location, fmd_tmp_location, fmd_runlumi, fmd_adler32, fmd_cksum, fmd_md5, fmd_lfn, fmd_size,\
               fmd_type, fmd_parent, fmd_creation_time, fmd_filestate, fmd_direct_stageout) \
               VALUES (:taskname, :pandajobid, :outdatasetname, :acquisitionera, :appver, :events, :globalTag,\
                       :publishdataname, :outlocation, :outtmplocation, :runlumi, :checksumadler32, :checksumcksum, :checksummd5, :outlfn, :outsize,\
                       :outtype, :inparentlfns, SYS_EXTRACT_UTC(SYSTIMESTAMP), :filestate, :directstageout)"

    DeleteTaskFiles_sql = "DELETE FROM filemetadata WHERE tm_taskname = :taskname"
    DeleteFilesByTime_sql = "DELETE FROM filemetadata WHERE fmd_creation_time < sysdate - (:hours/24)"
