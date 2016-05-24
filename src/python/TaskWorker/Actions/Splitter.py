import urllib
from base64 import b64encode
from httplib import HTTPException

from WMCore.DataStructs.Workflow import Workflow
from WMCore.DataStructs.Subscription import Subscription
from WMCore.JobSplitting.SplitterFactory import SplitterFactory

from RESTInteractions import HTTPRequests

from TaskWorker.DataObjects.Result import Result
from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.WorkerExceptions import TaskWorkerException


class Splitter(TaskAction):
    """Performing the split operation depending on the
       recevied input and arguments"""

    def execute(self, *args, **kwargs):
        wmwork = Workflow(name=kwargs['task']['tm_taskname'])

        wmsubs = Subscription(fileset=args[0], workflow=wmwork,
                               split_algo=kwargs['task']['tm_split_algo'],
                               type=self.jobtypeMapper[kwargs['task']['tm_job_type']])
        splitter = SplitterFactory()
        jobfactory = splitter(subscription=wmsubs)
        splitparam = kwargs['task']['tm_split_args']
        splitparam['algorithm'] = kwargs['task']['tm_split_algo']
        if kwargs['task']['tm_job_type'] == 'Analysis':
            if kwargs['task']['tm_split_algo'] == 'FileBased':
                splitparam['total_files'] = kwargs['task']['tm_totalunits']
            elif kwargs['task']['tm_split_algo'] == 'LumiBased':
                splitparam['total_lumis'] = kwargs['task']['tm_totalunits']
            elif kwargs['task']['tm_split_algo'] == 'EventAwareLumiBased':
                splitparam['total_events'] = kwargs['task']['tm_totalunits']
        elif kwargs['task']['tm_job_type'] == 'PrivateMC':
            if 'tm_events_per_lumi' in kwargs['task'] and kwargs['task']['tm_events_per_lumi']:
                splitparam['events_per_lumi'] = kwargs['task']['tm_events_per_lumi']
            if 'tm_generator' in kwargs['task'] and kwargs['task']['tm_generator'] == 'lhe':
                splitparam['lheInputFiles'] = True
        splitparam['applyLumiCorrection'] = True
        factory = jobfactory(**splitparam)
        numJobs = sum([len(jobgroup.getJobs()) for jobgroup in factory])
        maxJobs = getattr(self.config.TaskWorker, 'maxJobsPerTask', 10000)
        if numJobs == 0:
            msg  = "The CRAB3 server backend could not submit any job to the Grid scheduler:"
            msg += " Splitting task %s" % (kwargs['task']['tm_taskname'])
            if kwargs['task']['tm_input_dataset']:
                msg += " on dataset %s" % (kwargs['task']['tm_input_dataset'])
            msg += " with %s method does not generate any job" % (kwargs['task']['tm_split_algo'])
            raise TaskWorkerException(msg)
        elif numJobs > maxJobs:
            raise TaskWorkerException("The splitting on your task generated %s jobs. The maximum number of jobs in each task is %s" %
                                        (numJobs, maxJobs))
        #printing duplicated lumis if any
        lumiChecker = getattr(jobfactory, 'lumiChecker', None)
        if lumiChecker and lumiChecker.splitLumiFiles:
            self.logger.warning("The input dataset contains the following duplicated lumis %s" % lumiChecker.splitLumiFiles.keys())
            #TODO use self.uploadWarning
            try:
                userServer = HTTPRequests(self.server['host'], kwargs['task']['user_proxy'], kwargs['task']['user_proxy'], retry = 2,
                                          logger = self.logger)
                configreq = {'subresource': 'addwarning',
                             'workflow': kwargs['task']['tm_taskname'],
                             'warning': b64encode('The CRAB3 server backend detected lumis split across files in the input dataset.'
                                        ' Will apply the necessary corrections in the splitting algorithms. You can ignore this message.')}
                userServer.post(self.restURInoAPI + '/task', data = urllib.urlencode(configreq))
            except HTTPException as hte:
                self.logger.error(hte.headers)
                self.logger.warning("Cannot add warning to REST after finding duplicates")

        return Result(task = kwargs['task'], result = factory)


if __name__ == '__main__':
    splitparams = [{'halt_job_on_file_boundaries': False, 'algorithm': 'LumiBased', 'lumis_per_job': 2000, 'splitOnRun': False},
                   {'halt_job_on_file_boundaries': False, 'algorithm': 'LumiBased', 'lumis_per_job': 50, 'splitOnRun': False},
                   {'algorithm': 'FileBased', 'files_per_job': 2000, 'splitOnRun': False},
                   {'algorithm': 'FileBased', 'files_per_job': 50, 'splitOnRun': False},]
