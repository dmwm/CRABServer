import logging
import time
import commands
import json
from ast import literal_eval

#CRAB dependencies
from CRABInterface.Utils import conn_handler


class DataJobMetadata(object):
    @staticmethod
    def globalinit(dbapi):
        DataJobMetadata.api = dbapi

    def __init__(self):
        self.logger = logging.getLogger("CRABLogger.DataJobMetadata")

    def inject(self, *args, **kwargs):
        raise NotImplementedError

    def getFiles(self, taskname, filetype):
        binds = {'taskname': taskname, 'filetype': filetype}
        sql = """SELECT panda_job_id AS pandajobid,
                        fmd_outdataset AS outdataset,
                        fmd_acq_era AS acquisitionera,
                        fmd_sw_ver AS swversion,
                        fmd_in_events AS inevents,
                        fmd_global_tag AS globaltag,
                        fmd_publish_name AS publishname,
                        fmd_location AS location,
                        fmd_runlumi AS runlumi,
                        fmd_adler32 AS adler32,
                        fmd_cksum AS cksum,
                        fmd_md5 AS md5,
                        fmd_lfn AS lfn,
                        fmd_size AS filesize,
                        fmd_parent AS parents
                 FROM filemetadata
                 WHERE tm_taskname = :taskname AND fmd_type = :filetype"""
        rows = self.api.query(None, None, sql, **binds)
        for row in rows:
            yield {'taskname': taskname,
                   'filetype': filetype,
                   'pandajobid': row[0],
                   'outdataset': row[1],
                   'acquisitionera': row[2],
                   'swversion': row[3],
                   'inevents': row[4],
                   'globaltag': row[5],
                   'publishname': row[6],
                   'location': row[7],
                   'runlumi': literal_eval(row[8].read()),
                   'adler32': row[9],
                   'cksum': row[10],
                   'md5': row[11],
                   'lfn': row[12],
                   'filesize': row[13],
                   'parents': literal_eval(row[14].read()),}
