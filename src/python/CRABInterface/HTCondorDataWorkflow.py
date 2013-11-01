
import re
from ast import literal_eval

import htcondor

from WMCore.REST.Error import ExecutionError, InvalidParameter
from WMCore.WMSpec.WMTask import buildLumiMask
from CRABInterface.DataWorkflow import DataWorkflow
from Databases.TaskDB.Oracle.Task.ID import ID
from Databases.TaskDB.Oracle.JobGroup.GetJobGroupFromID import GetJobGroupFromID
from Databases.FileMetaDataDB.Oracle.FileMetaData.GetFromTaskAndType import GetFromTaskAndType
from CRABInterface.Utils import conn_handler

import HTCondorUtils
import HTCondorLocator

JOB_KILLED_HOLD_REASON = "Python-initiated action."

class HTCondorDataWorkflow(DataWorkflow):
    """ HTCondor implementation of the status command.
    """

    successList = ['finished']
    failedList = ['cancelled', 'failed']

    def status(self, workflow, userdn, userproxy=None):
        """Retrieve the status of the workflow.

           :arg str workflow: a valid workflow name
           :return: a workflow status summary document"""

        import cherrypy

        name = workflow.split("_")[0]
        cherrypy.log("Getting status for workflow %s, looking for schedd %s" %\
                                (workflow, name))
        locator = HTCondorLocator.HTCondorLocator(self.config)
        cherrypy.log("Will talk to %s." % locator.getCollector())
        name = locator.getSchedd()
        cherrypy.log("Schedd name %s." % name)
        schedd, address = locator.getScheddObj(name)

        results = self.getRootTasks(workflow, schedd)

        # TODO: handle case where there were no root tasks performed.
        # In such a case, we ought to check the DB.
        jobsPerStatus = {}
        jobStatus = {}
        jobList = []
        taskStatusCode = int(results[-1]['JobStatus'])
        taskJobCount = int(results[-1].get('CRAB_JobCount', 0))
        codes = {1: 'idle', 2: 'running', 3: 'killing', 4: 'completed', 5: 'cancelled'}
        task_codes = {1: 'SUBMITTED', 2: 'SUBMITTED', 4: 'COMPLETED', 5: 'KILLED'}
        retval = {"status": task_codes.get(taskStatusCode, 'unknown'), "taskFailureMsg": "", "jobSetID": workflow,
            "jobsPerStatus" : jobsPerStatus, "jobList": jobList}
        if taskStatusCode == 5 and \
                not results[-1]['HoldReason'].startswith(JOB_KILLED_HOLD_REASON):
            retval['status'] = 'InTransition'

        # Handle all "real" jobs in HTCondor.
        failedJobs = []
        allJobs = self.getHTCondorJobs(workflow, schedd)
        for result in allJobs:
            jobState = int(result['JobStatus'])
            if result['CRAB_Id'] in failedJobs:
                failedJobs.remove(result['CRAB_Id'])
            if (jobState == 4) and ('ExitCode' in result) and (int(result['ExitCode'])):
                failedJobs.append(result['CRAB_Id'])
                statusName = "failed"
            else:
                statusName = codes.get(jobState, 'unknown')
            jobStatus[int(result['CRAB_Id'])] = statusName

        # Handle all "finished" jobs.
        for result, file_state in self.getFinishedJobs(workflow):
            if result not in jobStatus:
                if file_state == 'FINISHED':
                    jobStatus[result] = 'finished'
                elif file_state == 'FAILED':
                    jobStatus[result] = 'failed'
                    failedJobs.append(result)
            if (result in failedJobs) and (file_state == 'FINISHED'):
                failedJobs.remove(result)
                jobStatus[result] = 'finished'

        for i in range(1, taskJobCount+1):
            if i not in jobStatus:
                if taskStatusCode == 5:
                    jobStatus[i] = 'killed'
                else:
                    jobStatus[i] = 'unsubmitted'

        for job, status in jobStatus.items():
            jobsPerStatus.setdefault(status, 0)
            jobsPerStatus[status] += 1
            jobList.append((status, job))

        retval["failedJobdefs"] = len(failedJobs)
        retval["totalJobdefs"] = len(jobStatus)

        if len(jobStatus) == 0 and taskJobCount == 0 and taskStatusCode == 2:
            retval['status'] = 'Running (jobs not submitted)'

        retval['jobdefErrors'] = []

        return [retval]

    def getRootTasks(self, workflow, schedd):
        rootConst = 'TaskType =?= "ROOT" && CRAB_ReqName =?= %s && (isUndefined(CRAB_Attempt) || CRAB_Attempt == 0)' % HTCondorUtils.quote(workflow)
        rootAttrList = ["JobStatus", "ExitCode", 'CRAB_JobCount', 'CRAB_ReqName', 'TaskType', "HoldReason"]

        # Note: may throw if the schedd is down.  We may want to think about wrapping the
        # status function and have it catch / translate HTCondor errors.
        results = schedd.query(rootConst, rootAttrList)

        if not results:
            self.logger.info("An invalid workflow name was requested: %s" % workflow)
            self.logger.info("Tried to read from address %s" % address)
            raise InvalidParameter("An invalid workflow name was requested: %s" % workflow)
        return results

    def getHTCondorJobs(self, workflow, schedd):
        """
        Retrieve all the jobs for executing cmsRun in this workflow
        """
        jobConst = 'TaskType =?= "Job" && CRAB_ReqName =?= %s' % HTCondorUtils.quote(workflow)
        jobList = ["JobStatus", 'ExitCode', 'ClusterID', 'ProcID', 'CRAB_Id']
        return schedd.query(jobConst, jobList)

    def getHTCondorASOJobs(self, workflow, schedd):
        jobConst = 'TaskType =?= "ASO" && CRAB_ReqName =?= %s' % HTCondorUtils.quote(workflow)
        jobList = ["JobStatus", 'ExitCode', 'ClusterID', 'ProcID', 'CRAB_Id']
        return schedd.query(jobConst, jobList)

    def getFinishedJobs(self, workflow):
        """
        Get the finished jobs from the file metadata table.
        """
        rows = self.api.query(None, None, GetFromTaskAndType.sql, filetype='FAKE', taskname=workflow)

        for row in rows:
            yield row[GetFromTaskAndType.PANDAID], row[GetFromTaskAndType.STATE]

    def logs(self, workflow, howmany, exitcode, jobids, userdn, userproxy=None):
        self.logger.info("About to get log of workflow: %s. Getting status first." % workflow)
        statusRes = self.status(workflow, userdn, userproxy)[0]

        transferingIds = [x[1] for x in statusRes['jobList'] if x[0] in ['transferring']]
        finishedIds = [x[1] for x in statusRes['jobList'] if x[0] in ['finished', 'failed']]
        return self.getFiles(workflow, howmany, jobids, ['LOG'], transferingIds, finishedIds, userdn, saveLogs=statusRes['saveLogs'], userproxy=userproxy)

    def output(self, workflow, howmany, jobids, userdn, userproxy=None):
        self.logger.info("About to get output of workflow: %s. Getting status first." % workflow)
        statusRes = self.status(workflow, userdn, userproxy)[0]

        transferingIds = [x[1] for x in statusRes['jobList'] if x[0] in ['transferring']]
        finishedIds = [x[1] for x in statusRes['jobList'] if x[0] in ['finished', 'failed']]
        return self.getFiles(workflow, howmany, jobids, ['EDM', 'TFILE'], transferingIds, finishedIds, userdn, userproxy=userproxy)

    @conn_handler(services=['phedex'])
    def getFiles(self, workflow, howmany, jobids, filetype, transferingIds, finishedIds, userdn, saveLogs=None, userproxy=None):
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
            if filetype == ['LOG'] and saveLogs == 'F':
                pfn = self.phedex.getPFN(row[GetFromTaskAndType.TMPLOCATION], row[GetFromTaskAndType.LFN])[(row[GetFromTaskAndType.TMPLOCATION], row[GetFromTaskAndType.LFN])]
            else:
                if row[GetFromTaskAndType.PANDAID] in finishedIds:
                    lfn = re.sub('^/store/temp/', '/store/', row[GetFromTaskAndType.LFN])
                    pfn = self.phedex.getPFN(row[GetFromTaskAndType.LOCATION], lfn)[(row[GetFromTaskAndType.LOCATION], lfn)]
                elif row[GetFromTaskAndType.PANDAID] in transferingIds:
                    pfn = self.phedex.getPFN(row[GetFromTaskAndType.TMPLOCATION], row[GetFromTaskAndType.LFN])[(row[GetFromTaskAndType.TMPLOCATION], row[GetFromTaskAndType.LFN])]
                else:
                    continue

            yield { 'pfn' : pfn,
                    'size' : row[GetFromTaskAndType.SIZE],
                    'checksum' : {'cksum' : row[GetFromTaskAndType.CKSUM], 'md5' : row[GetFromTaskAndType.ADLER32], 'adler32' : row[GetFromTaskAndType.ADLER32]}
            }

    def report(self, workflow, userdn, userproxy=None):
        res = {}
        self.logger.info("About to compute report of workflow: %s. Getting status first." % workflow)
        statusRes = self.status(workflow, userdn, userproxy)[0]

        #load the lumimask
        rows = self.api.query(None, None, ID.sql, taskname = workflow)
        splitArgs = literal_eval(rows.next()[6].read())
        res['lumiMask'] = buildLumiMask(splitArgs['runs'], splitArgs['lumis'])
        self.logger.info("Lumi mask was: %s" % res['lumiMask'])

        #extract the finished jobs from filemetadata
        jobids = [x[1] for x in statusRes['jobList'] if x[0] in ['finished', 'transferring']]
        rows = self.api.query(None, None, GetFromTaskAndType.sql, filetype='EDM', taskname=workflow)

        res['runsAndLumis'] = {}
        for row in rows:
            if row[GetFromTaskAndType.PANDAID] in jobids:
                res['runsAndLumis'][str(row[GetFromTaskAndType.PANDAID])] = { 'parents' : row[GetFromTaskAndType.PARENTS].read(),
                        'runlumi' : row[GetFromTaskAndType.RUNLUMI].read(),
                        'events'  : row[GetFromTaskAndType.INEVENTS],
                }
        self.logger.info("Got %s edm files for workflow %s" % (len(res), workflow))

        yield res
