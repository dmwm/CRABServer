""" This module implement the data part of the filemetadata API (RESTFilemetadata)
    Its main method is inject (that corerspond to a PUT) that INSERT or UPDATE a file
    (if it already exists)
"""

import json
import logging
from ast import literal_eval

from Utils.Utilities import makeList

from CRABInterface.Utilities import getDBinstance

class DataFileMetadata(object):
    """ DataFileMetadata class
    """
    @staticmethod
    def globalinit(dbapi, config):
        """ Called by RESTBaseAPI to initialize some parameters that are common between all the DataFileMetadata instances"""
        DataFileMetadata.api = dbapi
        DataFileMetadata.config = config

    def __init__(self, config):
        self.logger = logging.getLogger("CRABLogger.DataFileMetadata")
        self.FileMetaData = getDBinstance(config, 'FileMetaDataDB', 'FileMetaData')

    def getFiles(self, taskname, filetype, howmany=None, lfnList=None):
        """ Given a taskname, a filetype and a number return a list of filemetadata from this task
            if a list of lfn is give, it returns metadata for files in that list, otherwise
            returns metadata for at most howmany files (default is all filemetadata for this task)
        """
        self.logger.debug("Calling jobmetadata for task %s and filetype %s" % (taskname, filetype))
        if howmany == None:
            howmany = -1
        if not lfnList:
            binds = {'taskname': taskname, 'filetype': filetype, 'howmany': howmany}
            allRows = self.api.query_load_all_rows(None, None, self.FileMetaData.GetFromTaskAndType_sql, **binds)
        else:
            allRows = []
            lfns = makeList(lfnList)  # from a string to a python list of strings
            for lfn in lfns:
                binds = {'taskname': taskname, 'lfn': lfn}
                rows = self.api.query_load_all_rows(None, None, self.FileMetaData.GetFromTaskAndLfn_sql, **binds)
                for row in rows:  # above call returns a generator, but we want a list
                    allRows.append(row)
        for row in allRows:
            row = self.FileMetaData.GetFromTaskAndType_tuple(*row)
            filedict = {
                'taskname': taskname,
                'filetype': row.type,
                'jobid': row.jobid,
                    'outdataset': row.outdataset,
                    'acquisitionera': row.acquisitionera,
                    'swversion': row.swversion,
                    'inevents': row.inevents,
                    'globaltag': row.globaltag,
                    'publishname': row.publishname,
                    'location': row.location,
                    'tmplocation': row.tmplocation,
                    'runlumi': literal_eval(row.runlumi),
                    'adler32': row.adler32,
                    'cksum': row.cksum,
                    'md5': row.md5,
                    'lfn': row.lfn,
                    'filesize': row.filesize,
                    'parents': literal_eval(row.parents),
                    'state': row.state,
                    'created': str(row.parents),
                    'tmplfn': row.tmplfn
            }
            yield json.dumps(filedict)

    def inject(self, **kwargs):
        """ Insert or update a record in the database
        """
        self.logger.debug("Calling jobmetadata inject with parameters %s" % kwargs)

        bindnames = set(kwargs.keys()) - set(['outfileruns', 'outfilelumis'])
        binds = {}
        for name in bindnames:
            binds[name] = [kwargs[name]]

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
        for lumis in outfilelumis:
            lumiDict = dict((lumiEvents.split(":")) for lumiEvents in lumis.split(","))
            lumiEventList.append(lumiDict)
        runList = kwargs['outfileruns']
        # fmd_runlumi column in FILEMETADATA table is CLOB, so need to cast into a string here
        binds['runlumi'] = [str(dict(zip(runList, lumiEventList)))]

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

    def changeState(self, **kwargs): #kwargs are (taskname, outlfn, filestate)
        """ UNUSED method that change the fmd_filestate column of a filemetadata record
        """
        self.logger.debug("Changing state of file %(outlfn)s in task %(taskname)s to %(filestate)s" % kwargs)

        self.api.modify(self.FileMetaData.ChangeFileState_sql, **dict((k, [v]) for k, v in kwargs.items()))

    def delete(self, taskname, hours):
        """ UNUSED method that deletes record from the FILEMETADATA table
            The database old record cleanup is done through database partitioning
        """
        if taskname:
            self.logger.debug("Deleting all the files associated to task: %s" % taskname)
            self.api.modifynocheck(self.FileMetaData.DeleteTaskFiles_sql, taskname=[taskname])
        if hours:
            self.logger.debug("Deleting all the files older than %s hours" % hours)
            self.api.modifynocheck(self.FileMetaData.DeleteFilesByTime_sql, hours=[hours])

    @staticmethod
    def preprocessLumiStrings(lumis):
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
