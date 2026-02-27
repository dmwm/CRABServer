#! /usr/bin/env python3

from __future__ import print_function
from __future__ import division

import os
import subprocess
try:
    from http.client import HTTPException  # Python 3 and Python 2 in modern CMSSW
except:  # pylint: disable=bare-except
    from httplib import HTTPException  # old Python 2 version in CMSSW_7

from CRABAPI.RawCommand import crabCommand
from CRABClient.ClientExceptions import ClientException

def crab_cmd(configuration):

    try:
        output = crabCommand(configuration['cmd'], **configuration['args'])
        return output
    except HTTPException as hte:
        print('Failed', configuration['cmd'], 'of the task: %s' % (hte.headers))
    except ClientException as cle:
        print('Failed', configuration['cmd'], 'of the task: %s' % (cle))


def parse_result(listOfTasks, checkPublication=False):

    testResult = []

    for task in listOfTasks:
        task['pubSummary'] = 'None'  # make sure this is initialized
        needToResubmit = False
        if task['dbStatus'] == 'SUBMITTED':
            # remove failed probe jobs (job id of X-Y kind) if any from count
            for job in task['jobs'].keys():
                if '-' in job and task['jobs'][job]['State'] == 'failed':
                    task['jobsPerStatus']['failed'] -= 1
            total_jobs = sum(task['jobsPerStatus'].values())
            finished_jobs = task['jobsPerStatus']['finished'] if 'finished' in task['jobsPerStatus'] else 0
            published_in_transfersdb = task['publication']['done'] if 'done' in task['publication'] else 0
            failedPublications = task['publication']['failed'] if 'failed' in task['publication'] else 0
            # deal with absurd format (output of a print command !) of outdatasets
            # task['outdatasets'] is of type string and it is like ['datasetname']
            # with string first character being '['
            # beware that soon after submission this is not defined.
            # reference: https://github.com/dmwm/CRABClient/blob/549c4e3b6158e8344315437d1d128f2288551d47/src/python/CRABClient/Commands/status.py#L1059
            outdataset = eval(task['outdatasets'])[0] if task['outdatasets'] else None
            if task['outdatasets'] == 'None':
                outdataset = None
            if outdataset:
                cmd = "/cvmfs/cms.cern.ch/common/dasgoclient --query"
                cmd += " 'file dataset=%s instance=prod/phys03' | wc -l" % outdataset
                ret = subprocess.check_output(cmd, shell=True)
                published_in_dbs = eval(ret)  # dirty trick from b'999\n' to 999 (e.g.)
            else:
                published_in_dbs = 0

            task['pubSummary'] = '%d/%d/%d' % (failedPublications, published_in_transfersdb, published_in_dbs)

            if ('finished', total_jobs) in task['jobsPerStatus'].items():
                if task['status'] == 'COMPLETED':
                    result = 'TestPassed'
                else:
                    needToResubmit = True
                if checkPublication and 'nopublication' not in task['taskName']:
                    # remove probe jobs (job id of X-Y kind) if any from count
                    jobsToPublish = total_jobs
                    for job in task['jobs'].keys():
                        if '-' in job:
                            jobsToPublish -= 1
                    if published_in_transfersdb == jobsToPublish and published_in_dbs == jobsToPublish:
                        result = 'TestPassed'
                    elif failedPublications:
                        result = 'TestFailed'
                    else:
                        result = 'TestRunning'
            elif any(k in task['jobsPerStatus'] for k in ('failed', 'held')):
                needToResubmit = True
            else:
                result = 'TestRunning'
        elif task['dbStatus'] in ['HOLDING', 'QUEUED', 'NEW']:
            result = 'TestRunning'
        else:
            needToResubmit = True
        if needToResubmit:
            resubmit = crab_cmd({'cmd': 'resubmit', 'args': {'dir': task['workdir']}})
            result = 'TestResubmitted'

        testResult.append({'TN': task['taskName'], 'testResult': result, 'dbStatus': task['dbStatus'],
                           'combinedStatus': task['status'], 'jobsPerStatus': task['jobsPerStatus'],
                           'publication (fail/tdb/DBS)': task['pubSummary']})

    return testResult


def main():
    """
    start from a list of task names in $WORK_DIR/artifacts/submitted_tasks (one per row)
    then will write in artifacts/result one line per task in dict-like format
    {'TN': taskname, 'dbStatus': 'SUBMITTED', 'testResult': 'TestPassed',
       'jobsPerStatus': {'finished': 100, ....},  'combinedStatus': 'COMPLETED'}
    Note that order of keys in the printout will be potentially random
    Jenkins will check this file for the Test* strings and
    if TestRunning is present: run the test again after some time
    if TestFailed is present" declare the test FAILED
    """
    listOfTasks = []
    instance = os.getenv('REST_Instance', 'preprod')
    work_dir = os.getenv('WORK_DIR', 'dummy_workdir')
    Check_Publication_Status = os.getenv('Check_Publication_Status', 'No')
    print("Check_Publication_Status is : ", Check_Publication_Status )
    checkPublication = True if Check_Publication_Status == 'Yes' else False

    with open('%s/artifacts/submitted_tasks' %work_dir) as fp:
        tasks = fp.readlines()

    for task in tasks:
        # when testing it helps to reuse already made directories
        remake_dir = '_'.join(task.rstrip().split('_')[-3:])
        if not os.path.isdir(remake_dir):
            remake_dict = {'task': task, 'instance': instance}
            outputDict = crab_cmd({'cmd': 'remake', 'args': remake_dict})
            remake_dir = outputDict['workDir']

        status_dict = {'dir': remake_dir}
        status_command_output = crab_cmd({'cmd': 'status', 'args': status_dict})
        status_command_output.update({'taskName': task.rstrip()})
        status_command_output['workdir'] = remake_dir
        listOfTasks.append(status_command_output)

    summary = parse_result(listOfTasks,checkPublication)

    with open('%s/artifacts/result' %work_dir, 'w') as fp:
        for result in summary:
            fp.write("%s\n" % result)

if __name__ == '__main__':
    main()
