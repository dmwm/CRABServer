#!/usr/bin/env python

import logging

class FileMetaData(object):
    """
    """

    ChangeFileState_sql = """UPDATE filemetadata SET fmd_filestate=%(filestate)s 
                             WHERE fmd_lfn=%(outlfn)s and tm_taskname=%(taskname)s """

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
                    AND FIND_IN_SET(fmd_type, %(filetype)s)
                    ORDER BY fmd_creation_time DESC
             """

    New_sql = "INSERT INTO filemetadata ( \
               tm_taskname, panda_job_id, fmd_outdataset, fmd_acq_era, fmd_sw_ver, fmd_in_events, fmd_global_tag,\
               fmd_publish_name, fmd_location, fmd_tmp_location, fmd_runlumi, fmd_adler32, fmd_cksum, fmd_md5, fmd_lfn, fmd_size,\
               fmd_type,fmd_parent,fmd_creation_time,fmd_filestate) \
               VALUES (%(taskname)s, %(pandajobid)s, %(outdatasetname)s, %(acquisitionera)s, %(appver)s, %(events)s, %(globalTag)s,\
                       %(publishdataname)s, %(outlocation)s, %(outtmplocation)s, %(runlumi)s, %(checksumadler32)s, %(checksumcksum)s, %(checksummd5)s, %(outlfn)s, %(outsize)s,\
                       %(outtype)s, %(inparentlfns)s, UTC_TIMESTAMP(), %(filestate)s)"
