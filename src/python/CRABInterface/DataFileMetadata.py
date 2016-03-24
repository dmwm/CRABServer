import json
import logging
from ast import literal_eval

from CRABInterface.Utils import getDBinstance

class DataFileMetadata(object):
    @staticmethod
    def globalinit(dbapi, config):
        DataFileMetadata.api = dbapi
        DataFileMetadata.config = config

    def __init__(self, config):
        self.logger = logging.getLogger("CRABLogger.DataFileMetadata")
        self.FileMetaData = getDBinstance(config, 'FileMetaDataDB', 'FileMetaData')

    def getFiles(self, taskname, filetype):
        self.logger.debug("Calling jobmetadata for task %s and filetype %s" % (taskname, filetype))
        binds = {'taskname': taskname, 'filetype': filetype}
        rows = self.api.query(None, None, self.FileMetaData.GetFromTaskAndType_sql, **binds)
        for row in rows:
            row = self.FileMetaData.GetFromTaskAndType_sql(*row)
            yield json.dumps({'taskname': taskname,
                   'filetype': filetype,
                   'pandajobid': row.pandajobid,
                   'jobid': row.jobid,
                   'outdataset': row.outdataset,
                   'acquisitionera': row.acquisitionera,
                   'swversion': row.swversion,
                   'inevents': row.inevents,
                   'globaltag': row.globaltag,
                   'publishname': row.publishname,
                   'location': row.location,
                   'tmplocation': row.tmplocation,
                   'runlumi': literal_eval(row.runlumi.read()),
                   'adler32': row.adler32,
                   'cksum': row.cksum,
                   'md5': row.md5,
                   'lfn': row.lfn,
                   'filesize': row.filesize,
                   'parents': literal_eval(row.parents.read()),
                   'state': row.state,
                   'created': str(row.parents),
                   'tmplfn': row.tmplfn})

    def inject(self, *args, **kwargs):
        self.logger.debug("Calling jobmetadata inject with parameters %s" % kwargs)

        bindnames = set(kwargs.keys()) - set(['outfileruns', 'outfilelumis'])
        binds = {}
        for name in bindnames:
            binds[name] = [str(kwargs[name])]
        binds['runlumi'] = [str(dict(list(zip(map(str, kwargs['outfileruns']), [map(str, lumilist.split(',')) for lumilist in kwargs['outfilelumis']]))))]

        #Changed to Select if exist, update, else insert
        binds['outtmplfn'] = binds['outlfn']
        row = self.api.query(None, None, self.FileMetaData.GetCurrent_sql, 
                              outlfn=binds['outlfn'][0], taskname=binds['taskname'][0])
        try:
            #just one row is picked up by the previous query
            row = next(row)
        except StopIteration:
            #StipIteration will be raised if no rows was found
            self.logger.debug('No rows selected. Inserting new row into filemetadata')
            self.api.modify(self.FileMetaData.New_sql, **binds)
            return []
        self.logger.debug('Changing filemetadata information about job %s' % row)
        update_bind = {}
        update_bind['outtmplocation'] = binds['outtmplocation']
        update_bind['outsize'] = binds['outsize']
        update_bind['taskname'] = binds['taskname']
        update_bind['outlfn'] = binds['outlfn']
        update_bind['outtmplfn'] = binds['outlfn']
        self.api.modify(self.FileMetaData.Update_sql, **update_bind)
        return []

    def changeState(self, *args, **kwargs):#kwargs are (taskname, outlfn, filestate)
        self.logger.debug("Changing state of file %(outlfn)s in task %(taskname)s to %(filestate)s" % kwargs)

        self.api.modify(self.FileMetaData.ChangeFileState_sql, **dict((k, [v]) for k, v in kwargs.iteritems()))

    def delete(self, taskname, hours):
        if taskname:
            self.logger.debug("Deleting all the files associated to task: %s" % taskname)
            self.api.modifynocheck(self.FileMetaData.DeleteTaskFiles_sql, taskname=[taskname])
        if hours:
            self.logger.debug("Deleting all the files older than %s hours" % hours)
            self.api.modifynocheck(self.FileMetaData.DeleteFilesByTime_sql, hours=[hours])
