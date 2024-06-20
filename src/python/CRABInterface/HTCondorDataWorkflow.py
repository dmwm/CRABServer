# pylint: disable=invalid-name
#from __future__ import absolute_import

from ast import literal_eval

from CRABInterface.DataWorkflow import DataWorkflow
from CRABInterface.Utilities import conn_handler
from ServerUtilities import PUBLICATIONDB_STATES
from Databases.FileMetaDataDB.Oracle.FileMetaData.FileMetaData import GetFromTaskAndType

# WMCore Utils module
from Utils.Throttled import UserThrottle
throttle = UserThrottle(limit=3)

class HTCondorDataWorkflow(DataWorkflow):
    """ HTCondor implementation of the "workflow" API.
    """

    def logs2(self, workflow, howmany, jobids):
        self.logger.info("About to get log of workflow: %s." % workflow)
        return self.getFiles2(workflow, howmany, jobids, ['LOG'])

    def output2(self, workflow, howmany, jobids):
        self.logger.info("About to get output of workflow: %s." % workflow)
        return self.getFiles2(workflow, howmany, jobids, ['EDM', 'TFILE', 'FAKE'])

    def getFiles2(self, workflow, howmany, jobids, filetype):
        """
        Retrieves the output PFN aggregating output in final and temporary locations.

        :arg str workflow: the unique workflow name
        :arg int howmany: the limit on the number of PFN to return
        :return: a generator of list of outputs"""

        # Looking up task information from the DB
        row = next(self.api.query(None, None, self.Task.ID_sql, taskname = workflow))
        row = self.Task.ID_tuple(*row)

        file_type = 'log' if filetype == ['LOG'] else 'output'

        self.logger.debug("Retrieving the %s files of the following jobs: %s" % (file_type, jobids))
        rows = self.api.query_load_all_rows(None, None, self.FileMetaData.GetFromTaskAndType_sql, filetype = ','.join(filetype), taskname = workflow, howmany = howmany)

        for row in rows:
            yield {'jobid': row[GetFromTaskAndType.JOBID],
                   'lfn': row[GetFromTaskAndType.LFN],
                   'site': row[GetFromTaskAndType.LOCATION],
                   'tmplfn': row[GetFromTaskAndType.TMPLFN],
                   'tmpsite': row[GetFromTaskAndType.TMPLOCATION],
                   'directstageout': row[GetFromTaskAndType.DIRECTSTAGEOUT],
                   'size': row[GetFromTaskAndType.SIZE],
                   'checksum' : {'cksum' : row[GetFromTaskAndType.CKSUM], 'md5' : row[GetFromTaskAndType.ADLER32], 'adler32' : row[GetFromTaskAndType.ADLER32]}
                  }

    def report2(self, workflow, userdn):
        """
        Queries the TaskDB for the webdir, input/output datasets, publication flag.
        Also gets input/output file metadata from the FileMetaDataDB.

        Any other information that the client needs to compute the report is available
        without querying the server.
        """

        res = {}

        ## Get the jobs status first.
        self.logger.info("Fetching report2 information for workflow %s. Getting status first." % (workflow))

        ## Get the information we need from the Task DB.
        row = next(self.api.query(None, None, self.Task.ID_sql, taskname = workflow))
        row = self.Task.ID_tuple(*row)

        outputDatasets = literal_eval(row.output_dataset.read() if row.output_dataset else '[]')
        publication = True if row.publication == 'T' else False

        res['taskDBInfo'] = {"userWebDirURL": row.user_webdir, "inputDataset": row.input_dataset,
                             "outputDatasets": outputDatasets, "publication": publication}

        ## What each job has processed
        ## ---------------------------
        ## Retrieve the filemetadata of output and input files. (The filemetadata are
        ## uploaded by the post-job after stageout has finished for all output and log
        ## files in the job.)
        rows = self.api.query_load_all_rows(None, None, self.FileMetaData.GetFromTaskAndType_sql, filetype='EDM,TFILE,FAKE,POOLIN', taskname=workflow, howmany=-1)
        # Return only the info relevant to the client.
        res['runsAndLumis'] = {}
        for row in rows:
            jobidstr = row[GetFromTaskAndType.JOBID]
            retRow = {'parents': row[GetFromTaskAndType.PARENTS].read(),
                      'runlumi': row[GetFromTaskAndType.RUNLUMI].read(),
                      'events': row[GetFromTaskAndType.INEVENTS],
                      'type': row[GetFromTaskAndType.TYPE],
                      'lfn': row[GetFromTaskAndType.LFN],
                      }
            if jobidstr not in res['runsAndLumis']:
                res['runsAndLumis'][jobidstr] = []
            res['runsAndLumis'][jobidstr].append(retRow)

        yield res

    def taskads(self, workflow):
        """ deprecated. Remove when https://github.com/dmwm/CRABClient/pull/5318 in production """
        return [{'JobStatus': 0}]  # makes old client to print "Waiting for ... bootstrap"

    def status(self, workflow, userdn):
        """ deprecated. Remove when https://github.com/dmwm/CRABClient/pull/5318 in production """
        # Empty results
        result = {"status": '',  # from the db
                  "command": '',  # from the db
                  "taskFailureMsg": '',  # from the db
                  "taskWarningMsg": [],  # from the db
                  "submissionTime": 0,  # from the db
                  "statusFailureMsg": '',  # errors of the status itself
                  "jobList": [],
                  "schedd": '',  # from the db
                  "splitting": '',  # from the db
                  "taskWorker": '',  # from the db
                  "webdirPath": '',  # from the db
                  "username"         : ''} #from the db
        return [result]

    @conn_handler(services=[])
    def publicationStatus(self, workflow, user):
        """Here is what basically the function return, a dict called publicationInfo in the subcalls:
                publicationInfo['status']: something like {'publishing': 0, 'publication_failed': 0, 'not_published': 0, 'published': 5}.
                                           Later on goes into dictresult['publication'] before being returned to the client
                publicationInfo['status']['error']: String containing the error message if not able to contact oracle
                                                    Later on goes into dictresult['publication']['error']
                publicationInfo['failure_reasons']: errors of single files (not yet implemented for oracle..)
        """
        return self.publicationStatusOracle(workflow, user)

    def publicationStatusOracle(self, workflow, user):
        publicationInfo = {}

        #query oracle for the information
        binds = {}
        binds['username'] = user
        binds['taskname'] = workflow
        res = list(self.api.query(None, None, self.transferDB.GetTaskStatusForPublication_sql, **binds))

        #group results by state
        statusDict = {}
        for row in res:
            status = row[2]
            statusStr = PUBLICATIONDB_STATES[status].lower()
            if status != 5:
                statusDict[statusStr] = statusDict.setdefault(statusStr, 0) + 1

        #format and return
        publicationInfo['status'] = statusDict

        #Generic errors (like oracle errors) goes here. N.B.: single files errors should not go here
#        msg = "Publication information for oracle not yet implemented"
#        publicationInfo['status']['error'] = msg

        return publicationInfo
