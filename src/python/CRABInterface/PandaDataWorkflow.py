import PandaServerInterface as pserver
from CRABInterface.DataWorkflow import DataWorkflow
from Databases.TaskDB.Oracle.Task.ID import ID
from Databases.TaskDB.Oracle.JobGroup.GetJobGroupFromID import GetJobGroupFromID


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
        _, jobsetid, status, vogroup, vorole, taskFailure = row.next() #just one row is picked up by the previous query
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
            schedEC, res = pserver.getPandIDsWithJobID(jobID=jobdefid, user=userdn, vo='cms', group=vogroup, role=vorole, userproxy=userproxy, credpath=self.credpath)
            self.logger.debug("Status for jobdefid %s: %s" % (jobdefid, schedEC))
            if schedEC:
                jobDefErrs.append("Cannot get information for jobdefid %s. Panda server error: %s" % (jobdefid, schedEC))
                self.logger.debug(jobDeErrs[-1])
                failedJobdefs += 1
                continue

            #prepare the result at the job granularity
            self.logger.debug("Iterating on: %s" % res)
            for jobid, (jobstatus, _) in res.iteritems():
                jobsPerStatus[jobstatus] = jobsPerStatus[jobstatus]+1 if jobstatus in jobsPerStatus else 1
                jobList.append((jobstatus,jobid))

        status = self._updateTaskStatus(workflow, status, jobsPerStatus)

        return [ {"status" : status,\
                  "taskFailureMsg" : taskFailure.read() if taskFailure else '',\
                  "jobSetID"        : jobsetid if jobsetid else '',\
                  "jobsPerStatus"   : jobsPerStatus,\
                  "failedJobdefs"   : failedJobdefs,\
                  "totalJobdefs"    : totalJobdefs,\
                  "jobdefErrors"    : jobDefErrs,\
                  "jobList"         : jobList }]
