import urllib
from base64 import b64encode
from httplib import HTTPException

from WMCore.DataStructs.Workflow import Workflow
from WMCore.DataStructs.Subscription import Subscription
from WMCore.JobSplitting.SplitterFactory import SplitterFactory

from TaskWorker.DataObjects.Result import Result
from TaskWorker.WorkerExceptions import StopHandler
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
        elif kwargs['task']['tm_job_type'] == 'PrivateMC':
            if 'tm_events_per_lumi' in kwargs['task'] and kwargs['task']['tm_events_per_lumi']:
                splitparam['events_per_lumi'] = kwargs['task']['tm_events_per_lumi']
            if 'tm_generator' in kwargs['task'] and kwargs['task']['tm_generator'] == 'lhe':
                splitparam['lheInputFiles'] = True
        factory = jobfactory(**splitparam)
        if len(factory) == 0:
            raise TaskWorkerException("The CRAB3 server backend could not submit any job to the Grid scheduler:\n"+\
                        "splitting task %s on dataset %s with %s method does not generate any job")

        return Result(task = kwargs['task'], result = factory)


if __name__ == '__main__':
    splitparams = [{'halt_job_on_file_boundaries': False, 'algorithm': 'LumiBased', 'lumis_per_job': 2000, 'splitOnRun': False},
                   {'halt_job_on_file_boundaries': False, 'algorithm': 'LumiBased', 'lumis_per_job': 50, 'splitOnRun': False},
                   {'algorithm': 'FileBased', 'files_per_job': 2000, 'splitOnRun': False},
                   {'algorithm': 'FileBased', 'files_per_job': 50, 'splitOnRun': False},]
