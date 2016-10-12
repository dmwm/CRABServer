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

    def getFiles(self, taskname, filetype, howmany):
        self.logger.debug("Calling jobmetadata for task %s and filetype %s" % (taskname, filetype))
        if howmany == None:
            howmany = -1
        binds = {'taskname': taskname, 'filetype': filetype, 'howmany': howmany}
        rows = self.api.query(None, None, self.FileMetaData.GetFromTaskAndType_sql, **binds)
        for row in rows:
            yield json.dumps({'taskname': taskname,
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
                   'parents': literal_eval(row[15].read()),
                   'state': row[16],
                   'created': str(row[17]),
                   'tmplfn': row[18]})

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
