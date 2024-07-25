"""
Split a task request into a set of jobs
"""
# pylint: disable=too-many-branches
from WMCore.DataStructs.Workflow import Workflow
from WMCore.DataStructs.Subscription import Subscription
from WMCore.JobSplitting.SplitterFactory import SplitterFactory

from TaskWorker.DataObjects.Result import Result
from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.WorkerExceptions import SubmissionRefusedException


class Splitter(TaskAction):
    """Performing the split operation depending on the
       recevied input and arguments"""

    def execute(self, *args, **kwargs):
        wmwork = Workflow(name=kwargs['task']['tm_taskname'])

        maxJobs = getattr(self.config.TaskWorker, 'maxJobsPerTask', 10000)

        data = args[0]
        splitparam = kwargs['task']['tm_split_args']
        splitparam['algorithm'] = kwargs['task']['tm_split_algo']
        if kwargs['task']['tm_job_type'] == 'Analysis':
            totalUnits = kwargs['task']['tm_totalunits']
            if kwargs['task']['tm_split_algo'] == 'FileBased':
                if totalUnits < 1.0:
                    totalUnits = int(totalUnits * len(data.getFiles()) + 0.5)
                splitparam['total_files'] = totalUnits
            elif kwargs['task']['tm_split_algo'] == 'LumiBased':
                if totalUnits < 1.0:
                    totalUnits = int(totalUnits * sum(len(run.lumis) for f in data.getFiles() for run in f['runs']) + 0.5)
                splitparam['total_lumis'] = totalUnits
            elif kwargs['task']['tm_split_algo'] == 'EventAwareLumiBased':
                if totalUnits < 1.0:
                    totalUnits = int(totalUnits * sum(f['events'] for f in data.getFiles()) + 0.5)
                splitparam['total_events'] = totalUnits
            elif kwargs['task']['tm_split_algo'] == 'Automatic':
                # REST backwards compatibility fix
                if 'seconds_per_job' in kwargs['task']['tm_split_args']:
                    kwargs['task']['tm_split_args']['minutes_per_job'] = kwargs['task']['tm_split_args'].pop('seconds_per_job')
                splitparam['algorithm'] = 'FileBased'
                splitparam['total_files'] = len(data.getFiles())
                numProbes = getattr(self.config.TaskWorker, 'numAutomaticProbes', 5)
                splitparam['files_per_job'] = (len(data.getFiles()) + numProbes - 1) // numProbes
        elif kwargs['task']['tm_job_type'] == 'PrivateMC':
            # sanity check
            nJobs = kwargs['task']['tm_totalunits'] // splitparam['events_per_job']
            if nJobs > maxJobs:
                raise SubmissionRefusedException(
                    f"Your task would generate {nJobs} jobs. The maximum number of jobs in each task is {maxJobs}"
                )
            if 'tm_events_per_lumi' in kwargs['task'] and kwargs['task']['tm_events_per_lumi']:
                splitparam['events_per_lumi'] = kwargs['task']['tm_events_per_lumi']
            if 'tm_generator' in kwargs['task'] and kwargs['task']['tm_generator'] == 'lhe':
                splitparam['lheInputFiles'] = True
        splitparam['applyLumiCorrection'] = True

        wmsubs = Subscription(fileset=data, workflow=wmwork,
                              split_algo=splitparam['algorithm'],
                              type=self.jobtypeMapper[kwargs['task']['tm_job_type']])
        try:
            splitter = SplitterFactory()
            jobfactory = splitter(subscription=wmsubs)
            factory = jobfactory(**splitparam)
            numJobs = sum([len(jobgroup.getJobs()) for jobgroup in factory])
        except RuntimeError:
            msg = f"The splitting on your task generated more than {maxJobs} jobs (the maximum)."
            raise SubmissionRefusedException(msg) from RuntimeError
        if numJobs == 0:
            msg = "CRAB could not submit any job to the Grid scheduler:"
            msg += f"\nsplitting task {kwargs['task']['tm_taskname']}"
            if kwargs['task']['tm_input_dataset']:
                msg += f"\non dataset {kwargs['task']['tm_input_dataset']}"
            msg += f"\nwith {kwargs['task']['tm_split_algo']} method does not generate any job. See\n"
            msg += "https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ#crab_submit_fails_with_Splitting"
            msg += diagnoseRunMatch(splitparam['runs'], data)
            raise SubmissionRefusedException(msg)
        if numJobs > maxJobs:
            raise SubmissionRefusedException(
                f"The splitting on your task generated {numJobs} jobs. The maximum number of jobs in each task is {maxJobs}"
            )

        minRuntime = getattr(self.config.TaskWorker, 'minAutomaticRuntimeMins', 180)
        if kwargs['task']['tm_split_algo'] == 'Automatic' and \
                kwargs['task']['tm_split_args']['minutes_per_job'] < minRuntime:
            msg = f"Minimum runtime requirement for automatic splitting is {minRuntime} minutes."
            raise SubmissionRefusedException(msg)

        # printing duplicated lumis if any
        lumiChecker = getattr(jobfactory, 'lumiChecker', None)
        if lumiChecker and lumiChecker.splitLumiFiles:
            self.logger.warning("The input dataset contains the following duplicated lumis %s", lumiChecker.splitLumiFiles.keys())
            msg = "CRAB detected lumis split across files in the input dataset."
            msg += " Will apply the necessary corrections in the splitting algorithm. You can ignore this message."
            self.uploadWarning(msg, kwargs['task']['user_proxy'], kwargs['task']['tm_taskname'])

        return Result(task=kwargs['task'], result=(factory, args[0]))


def diagnoseRunMatch(runs=None, data=None):
    """
    check matching of run list in the user's input and in the dataset
    runs: a list of run numbers from splitargs (i.e. crabConfing runRange and/or lumimask)
    data: a list of WMCore.DataStructs.File objects as prepared by DataDiscovery
    Returns a string with the message to send to the user
    """

    # use sets to make intersection and allow for multiple files to have same run #
    runsInConfig = set(runs)
    runsInData = set()
    for fileObj in data.getFiles():  # a list of WMCore.DataStructs.File objects
        for runObj in fileObj['runs']:  # a list of WMCore.DataStructs.Run objects
            runsInData.add(runObj.run)
    intersection = list(runsInData & runsInConfig)

    # now turn into sorted lists to compute ranges
    runsInConfig = sorted(list(runs))
    runsInData = sorted(list(runsInData))
    configRange = f"{runsInConfig[0]} - {runsInConfig[-1]}"
    dataRange = f"{runsInData[0]} - {runsInData[-1]}"

    msg = "\nSome hopefully helpful information to help you figure out the reason:"
    msg += f"\nRun list from task configuration is inside the range: {configRange}"
    msg += f"\nRun list from input dataset is inside the range: {dataRange}"
    msg += f"\nThe intersection of the two lists is: {intersection}"
    return msg


if __name__ == '__main__':
    splitparams = [{'halt_job_on_file_boundaries': False, 'algorithm': 'LumiBased', 'lumis_per_job': 2000, 'splitOnRun': False},
                   {'halt_job_on_file_boundaries': False, 'algorithm': 'LumiBased', 'lumis_per_job': 50, 'splitOnRun': False},
                   {'algorithm': 'FileBased', 'files_per_job': 2000, 'splitOnRun': False},
                   {'algorithm': 'FileBased', 'files_per_job': 50, 'splitOnRun': False}]
