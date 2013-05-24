import logging
import time
import commands
import json
from ast import literal_eval

# WMCore dependecies here
from WMCore.REST.Error import InvalidParameter, ExecutionError, MissingObject

#CRAB dependencies
from Databases.FileMetaDataDB.Oracle.FileMetaData.New import New

class DataJobMetadata(object):
    @staticmethod
    def globalinit(dbapi):
        DataJobMetadata.api = dbapi

    def __init__(self):
        self.logger = logging.getLogger("CRABLogger.DataJobMetadata")

    def getFiles(self, taskname, filetype):
        self.logger.debug("Calling jobmetadata for task %s and filetype %s" % (taskname, filetype))
        binds = {'taskname': taskname, 'filetype': filetype}
        sql = """SELECT panda_job_id AS pandajobid,
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
                   'tmplocation': row[8],
                   'runlumi': literal_eval(row[9].read()),
                   'adler32': row[10],
                   'cksum': row[11],
                   'md5': row[12],
                   'lfn': row[13],
                   'filesize': row[14],
                   'parents': literal_eval(row[15].read()),}

    def inject(self, *args, **kwargs):
        self.logger.debug("Calling jobmetadata inject with parameters %s" % kwargs)

        bindnames = set(kwargs.keys()) - set(['outfileruns', 'outfilelumis'])
        binds = {}
        for name in bindnames:
            binds[name] = [str(kwargs[name])]
        binds['runlumi'] = [str(dict(zip(map(str, kwargs['outfileruns']), [map(str, lumilist.split(',')) for lumilist in kwargs['outfilelumis']])))]

        self.api.modify(New.sql, **binds)
        return []

