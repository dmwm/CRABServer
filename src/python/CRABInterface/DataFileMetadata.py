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
            row = self.FileMetaData.GetFromTaskAndType_tuple(*row)
            yield json.dumps({'taskname': taskname,
                   'filetype': filetype,
                   #TODO pandajobid should not be used. Let's wait a "quiet release" and remove it
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

        # Modify all incoming metadata to have the structure 'lumi1:events1,lumi2:events2..'
        # instead of 'lumi1,lumi2,lumi3...' if necessary.
        # This provides backwards compatibility for old postjobs and also
        # changes the input metadata structure. Input metadata, however,
        # will always have 'None' as the number of events.
        outfilelumis = self.preprocessLumiStrings(kwargs['outfilelumis'])

        # Construct a dict like
        # {'runNum1': {'lumi1': 'events1', 'lumi2': 'events2'}, 'runNum2': {'lumi3': 'events3', 'lumi4': 'events4'}}
        # from two lists of runs and lumi info strings that look like:
        # runs: ['runNum1', 'runNum2']
        # lumi info: ['lumi1:events1,lumi2:events2', 'lumi3:events3,lumi4:events4]
        # By their index, run numbers correspond to the lumi info strings 
        # which is why we can use zip when creating the dict.
        lumiEventList = []
        for lumis in map(str, outfilelumis):
            lumiDict = dict((lumiEvents.split(":")) for lumiEvents in lumis.split(","))
            lumiEventList.append(lumiDict)
        # Convert runs to strings to keep it consistent
        strOutFileRuns = map(str, kwargs['outfileruns'])
        binds['runlumi'] = [str(dict(zip(strOutFileRuns, lumiEventList)))]

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

    def preprocessLumiStrings(self, lumis):
        """ Takes a list of strings of lumis and updates
            their format if necessary.
            Old: 'lumi1,lumi2,lumi3...'
            New: 'lumi1:events1,lumi2:events2...'

            If the format is old, the lumi strings will be
            "padded" with 'None' strings instead of event counts
            to show that information about events per each
            lumi is unavailable. This needs to be done when an old
            PostJob wants to upload the output metadata, but it does not have the code
            for parsing the events per lumi info and does not upload it to the server.

            Because input metadata uploads are using the same API, input metadata
            will also be padded to keep the format consistent. However, input metadata
            will always have 'None' instead of event counts because currently we do not
            care about input metadata event counts per each lumi.
        """
        res = []
        for lumistring in lumis:
            if ":" not in lumistring:
                res.append(','.join([lumi+":None" for lumi in lumistring.split(",")]))
            else:
                return lumis
        return res
