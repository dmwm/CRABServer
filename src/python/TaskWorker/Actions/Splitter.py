import urllib
from httplib import HTTPException
from base64 import b64encode

from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.Workflow import Workflow
from WMCore.JobSplitting.SplitterFactory import SplitterFactory

from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.DataObjects.Result import Result
from TaskWorker.WorkerExceptions import StopHandler


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
        factory = jobfactory(**splitparam)
        if len(factory) == 0:
            # Understanding that no jobs could be created given the splitting arguments
            # with the given input dataset information: NO IDEA WHY.
            # NB: we assume that split can't happen, then task is failed
            msg = "Splitting %s on %s with %s does not generate any job" %(kwargs['task']['tm_taskname'],
                                                                           kwargs['task']['tm_input_dataset'],
                                                                           kwargs['task']['tm_split_algo'])
            self.logger.error("Setting %s as failed" % str(kwargs['task']['tm_taskname']))
            configreq = {'workflow': kwargs['task']['tm_taskname'],
                         'status': "FAILED",
                         'subresource': 'failure',
                         'failure': b64encode(msg)}
            self.server.post(self.resturl, data = urllib.urlencode(configreq))
            raise StopHandler(msg)
        return Result(task=kwargs['task'], result=factory)


if __name__ == '__main__':
    splitparams = [{'halt_job_on_file_boundaries': False, 'algorithm': 'LumiBased', 'lumis_per_job': 2000, 'splitOnRun': False},
                   {'halt_job_on_file_boundaries': False, 'algorithm': 'LumiBased', 'lumis_per_job': 50, 'splitOnRun': False},
                   {'algorithm': 'FileBased', 'files_per_job': 2000, 'splitOnRun': False},
                   {'algorithm': 'FileBased', 'files_per_job': 50, 'splitOnRun': False},]
