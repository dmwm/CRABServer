import re
from ast import literal_eval
import PandaServerInterface as pserver
from WMCore.REST.Error import ExecutionError, InvalidParameter
from WMCore.WMSpec.WMTask import buildLumiMask
from CRABInterface.DataWorkflow import DataWorkflow
from Databases.TaskDB.Oracle.Task.ID import ID
from Databases.TaskDB.Oracle.JobGroup.GetJobGroupFromID import GetJobGroupFromID
from Databases.FileMetaDataDB.Oracle.FileMetaData.GetFromTaskAndType import GetFromTaskAndType
from CRABInterface.Utils import conn_handler

class PandaDataWorkflow(DataWorkflow):
    """ Panda implementation of the status command.
    """

    #List of states which in panda are considered completed (and therefore not killed)
    successList = ['finished']
    failedList = ['cancelled', 'failed']

    def status(self, workflow, userdn, userproxy=None):
        """Retrieve the status of the workflow.

           :arg str workflow: a valid workflow name
           :return: a workflow status summary document"""
        self.logger.debug("Getting status for workflow %s" % workflow)
        row = self.api.query(None, None, ID.sql, taskname = workflow)
        _, jobsetid, status, vogroup, vorole, taskFailure, splitArgs, resJobs  = row.next() #just one row is picked up by the previous query
        print resJobs
        resJobs = literal_eval(resJobs.read())
        self.logger.info("Status result for workflow %s: %s. JobsetID: %s" % (workflow, status, jobsetid))
        self.logger.debug("User vogroup=%s and user vorole=%s" % (vogroup, vorole))

        rows = self.api.query(None, None, GetJobGroupFromID.sql, taskname = workflow)
        jobsPerStatus = {}
        jobList = []
        totalJobdefs = 0
        failedJobdefs = 0
        jobDefErrs = []
        for jobdef in rows:
            jobdefid = jobdef[0]
            jobdefStatus = jobdef[1]
            jobdefError = jobdef[2].read() if jobdef[2] else ''
            totalJobdefs += 1
            self.logger.debug("DB Status for jobdefid %s is %s. %s" % (jobdefid, jobdefStatus, jobdefError))

            #check if the taskworker succeded in the submission
            if jobdefStatus == 'FAILED':
                jobDefErrs.append(jobdefError)
                failedJobdefs += 1
                continue

            #check the status of the jobdef in panda
            schedEC, res = pserver.getPandIDsWithJobID(jobID=jobdefid, dn=userdn, userproxy=userproxy, credpath=self.credpath)
            self.logger.debug("Status for jobdefid %s: %s" % (jobdefid, schedEC))
            if schedEC:
                jobDefErrs.append("Cannot get information for jobdefid %s. Panda server error: %s" % (jobdefid, schedEC))
                self.logger.debug(jobDeErrs[-1])
                failedJobdefs += 1
                continue

            #prepare the result at the job granularity
            self.logger.debug("Iterating on: %s" % res)
            for jobid, (jobstatus, _) in res.iteritems():
                if jobid not in resJobs:
                    jobsPerStatus[jobstatus] = jobsPerStatus[jobstatus]+1 if jobstatus in jobsPerStatus else 1
                    jobList.append((jobstatus,jobid))

        if set(jobsPerStatus): #if we actually submitted something
            status = self._updateTaskStatus(workflow, status, jobsPerStatus)

        return [ {"status" : status,\
                  "taskFailureMsg" : taskFailure.read() if taskFailure else '',\
                  "jobSetID"        : jobsetid if jobsetid else '',\
                  "jobsPerStatus"   : jobsPerStatus,\
                  "failedJobdefs"   : failedJobdefs,\
                  "totalJobdefs"    : totalJobdefs,\
                  "jobdefErrors"    : jobDefErrs,\
                  "jobList"         : jobList }]

    def logs(self, workflow, howmany, exitcode, jobids, userdn, userproxy=None):
        self.logger.info("About to get log of workflow: %s. Getting status first." % workflow)
        statusRes = self.status(workflow, userdn, userproxy)[0]

        transferingIds = [x[1] for x in statusRes['jobList'] if x[0] in ['transferring']]
        finishedIds = [x[1] for x in statusRes['jobList'] if x[0] in ['finished', 'failed']]
        return self.getFiles(workflow, howmany, jobids, ['LOG'], transferingIds, finishedIds, userdn, userproxy)

    def output(self, workflow, howmany, jobids, userdn, userproxy=None):
        self.logger.info("About to get output of workflow: %s. Getting status first." % workflow)
        statusRes = self.status(workflow, userdn, userproxy)[0]

        transferingIds = [x[1] for x in statusRes['jobList'] if x[0] in ['transferring']]
        finishedIds = [x[1] for x in statusRes['jobList'] if x[0] in ['finished', 'failed']]
        return self.getFiles(workflow, howmany, jobids, ['EDM', 'TFILE'], transferingIds, finishedIds, userdn, userproxy)

    @conn_handler(services=['phedex'])
    def getFiles(self, workflow, howmany, jobids, filetype, transferingIds, finishedIds, userdn, userproxy=None):
        """
        Retrieves the output PFN aggregating output in final and temporary locations.

        :arg str workflow: the unique workflow name
        :arg int howmany: the limit on the number of PFN to return
        :return: a generator of list of outputs"""

        #check that the jobids passed by the user are finished
        for jobid in jobids:
            if not jobid in transferingIds + finishedIds:
                raise InvalidParameter("The job with id %s is not finished" % jobid)

        #If the user do not give us jobids set them to all possible ids
        if not jobids:
            jobids = transferingIds + finishedIds
        else:
            howmany = -1 #if the user specify the jobids return all possible files with those ids

        #user did not give us ids and no ids available in the task
        if not jobids:
            self.logger.info("No finished jobs found in the task")
            return

        self.logger.debug("Retrieving output of jobs: %s" % jobids)
        rows = self.api.query(None, None, GetFromTaskAndType.sql, filetype=','.join(filetype), taskname=workflow)
        rows = filter(lambda row: row[GetFromTaskAndType.PANDAID] in jobids, rows)
        if howmany!=-1:
            rows=rows[:howmany]
        #jobids=','.join(map(str,jobids)), limit=str(howmany) if howmany!=-1 else str(len(jobids)*100))

        for row in rows:
            if row[GetFromTaskAndType.PANDAID] in finishedIds:
                lfn = re.sub('^/store/temp/', '/store/', row[GetFromTaskAndType.LFN])
                pfn = self.phedex.getPFN(row[GetFromTaskAndType.LOCATION], lfn)[(row[GetFromTaskAndType.LOCATION], lfn)]
            elif row[GetFromTaskAndType.PANDAID] in transferingIds:
                pfn = self.phedex.getPFN(row[GetFromTaskAndType.TMPLOCATION], row[GetFromTaskAndType.LFN])[(row[GetFromTaskAndType.TMPLOCATION], row[GetFromTaskAndType.LFN])]
            else:
                continue

            yield { 'pfn' : pfn,
                    'size' : row[GetFromTaskAndType.SIZE],
                    'checksum' : {'cksum' : row[GetFromTaskAndType.CKSUM], 'md5' : row[GetFromTaskAndType.ADLER32], 'adler31' : row[GetFromTaskAndType.ADLER32]}
            }

    def report(self, workflow, userdn, userproxy=None):
        res = {}
        self.logger.info("About to get output of workflow: %s. Getting status first." % workflow)
        statusRes = self.status(workflow, userdn, userproxy)[0]

        #load the lumimask
        rows = self.api.query(None, None, ID.sql, taskname = workflow)
        splitArgs = literal_eval(rows.next()[6].read())
        res['lumiMask'] = buildLumiMask(splitArgs['runs'], splitArgs['lumis'])

        #extract the finished jobs from filemetadata
        jobids = [x[1] for x in statusRes['jobList'] if x[0] in ['finished', 'transferring']]
        rows = self.api.query(None, None, GetFromTaskAndType.sql, filetype='EDM', taskname=workflow)
        rows = filter(lambda row: row[GetFromTaskAndType.PANDAID] in jobids, rows)

        res['runsAndLumis'] = {}
        for row in rows:
            res['runsAndLumis'][str(row[GetFromTaskAndType.PANDAID])] = { 'parents' : row[GetFromTaskAndType.PARENTS].read(),
                    'runlumi' : row[GetFromTaskAndType.RUNLUMI].read(),
                    'events'  : row[GetFromTaskAndType.INEVENTS],
            }

        yield res
