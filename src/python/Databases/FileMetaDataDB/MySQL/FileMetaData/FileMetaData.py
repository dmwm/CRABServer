#!/usr/bin/env python

import logging

class FileMetaData(object):
    """
    """

    ChangeFileState_sql = """UPDATE filemetadata SET fmd_filestate=%(filestate)s 
                             WHERE fmd_lfn=%(outlfn)s and tm_taskname=%(taskname)s """
   
    GetFromPandaIdsatter_sql = "SELECT fmd_lfn, fmd_location, fmd_tmp_location, fmd_size, fmd_cksum, fmd_md5, fmd_adler32, panda_job_id," +\
             "fmd_parent, fmd_runlumi, fmd_in_events FROM filemetadata WHERE " +\
             "fmd_type IN (SELECT REGEXP_SUBSTR(%(types)s, '[^,]+', 1, LEVEL) FROM DUAL CONNECT BY LEVEL <= REGEXP_COUNT(%(types)s, ',') + 1) AND " +\
             "panda_job_id IN (SELECT REGEXP_SUBSTR(%(jobids)s, '[^,]+', 1, LEVEL) FROM DUAL CONNECT BY LEVEL <= REGEXP_COUNT(%(jobids)s, ',') + 1) " +\
             "AND ROWNUM<=%(limit)s AND tm_taskname=%(taskname)s ORDER BY fmd_creation_time"
   
    GetFromTaskAndType_sql = """SELECT panda_job_id AS pandajobid,
                           fmd_outdataset AS outdataset,
                           fmd_acq_era AS acquisitionera,
                           fmd_sw_ver AS swversion,
                           fmd_in_events AS inevents,
                           fmd_global_tag AS globaltag,
                           fmd_publish_name AS publishname,
                           fmd_location AS location,
                           fmd_tmp_location AS tmplocation,
                           fmd_runlumi AS runlumi,
                           fmd_adler32 AS adler32,
                           fmd_cksum AS cksum,
                           fmd_md5 AS md5,
                           fmd_lfn AS lfn,
                           fmd_size AS filesize,
                           fmd_parent AS parents,
                           fmd_filestate AS state
                    FROM filemetadata
                    WHERE tm_taskname = %(taskname)s
                    AND fmd_type IN (SELECT REGEXP_SUBSTR(%(filetype)s, '[^,]+', 1, LEVEL) FROM DUAL CONNECT BY LEVEL <= REGEXP_COUNT(%(filetype)s, ',') + 1)
                    ORDER BY fmd_creation_time DESC
             """
   
    New_sql = "INSERT INTO filemetadata ( \
               tm_taskname, panda_job_id, fmd_outdataset, fmd_acq_era, fmd_sw_ver, fmd_in_events, fmd_global_tag,\
               fmd_publish_name, fmd_location, fmd_tmp_location, fmd_runlumi, fmd_adler32, fmd_cksum, fmd_md5, fmd_lfn, fmd_size,\
               fmd_type,fmd_parent,fmd_creation_time,fmd_filestate) \
               VALUES (%(taskname)s, %(pandajobid)s, %(outdatasetname)s, %(acquisitionera)s, %(appver)s, %(events)s, %(globalTag)s,\
                       %(publishdataname)s, %(outlocation)s, %(outtmplocation)s, %(runlumi)s, %(checksumadler32)s, %(checksumcksum)s, %(checksummd5)s, %(outlfn)s, %(outsize)s,\
                       %(outtype)s, %(inparentlfns)s, UTC_TIMESTAMP(), %(filestate)s)"
