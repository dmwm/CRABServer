
from DashboardAPI import apmonSend, apmonFree
    
class ApmonIf:
    """
    Provides an interface to the Monalisa Apmon python module
    """
    def __init__(self, taskid=None, jobid=None) :
        self.taskId = taskid
        self.jobId = jobid
        self.fName = 'mlCommonInfo'

    #def fillDict(self, parr):
    #    """
    #    Obsolete
    #    """
    #    pass
    
    def sendToML(self, params, jobid=None, taskid=None):
        # Figure out taskId and jobId
        taskId = 'unknown'
        jobId = 'unknown'
        # taskId
        if self.taskId is not None :
            taskId = self.taskId
        if params.has_key('taskId') :
            taskId = params['taskId']
        if taskid is not None :
            taskId = taskid
        # jobId
        if self.jobId is not None :
            jobId = self.jobId
        if params.has_key('jobId') :
            jobId = params['jobId']
        if jobid is not None :
            jobId = jobid
        # Send to Monalisa
        apmonSend(taskId, jobId, params)
            
    def free(self):
        apmonFree()

