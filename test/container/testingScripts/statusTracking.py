#! /usr/bin/env python

from __future__ import print_function
from __future__ import division

import os
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
        

def parse_result(listOfTasks):

    testResult = []

    for task in listOfTasks:
        if task['dbStatus'] == 'SUBMITTED' and task['status'] != 'FAILED':
            # remove failed probe jobs (job id of X-Y kind) if any from count
            for job in task['jobs'].keys():
                if '-' in job and task['jobs'][job]['State'] == 'failed':
                    task['jobsPerStatus']['failed'] -= 1
            total_jobs = sum(task['jobsPerStatus'].values())
            finished_jobs = task['jobsPerStatus']['finished'] if 'finished' in task['jobsPerStatus'] else 0
            published_in_transfersdb = task['publication']['done'] if 'done' in task['publication'] else 0
            published_in_dbs = 0  # TODO make a call to dasgoclient
            task['pubSummary'] = '%d/%d/%d' % (published_in_dbs, published_in_transfersdb, finished_jobs )

            if ('finished', total_jobs) in task['jobsPerStatus'].items():
                result = 'TestPassed'
            elif any(k in task['jobsPerStatus'] for k in ('failed', 'held')):
                resubmit = crab_cmd({'cmd': 'resubmit', 'args': {'dir': task['workdir']}})
                result = 'TestResubmitted'
            else:
                result = 'TestRunning'
        elif task['dbStatus'] in ['HOLDING', 'QUEUED', 'NEW']:
            result = 'TestRunning'
        else:
            result = 'TestFailed'

        testResult.append({'TN': task['taskName'], 'testResult': result, 'dbStatus': task['dbStatus'], 
                           'combinedStatus': task['status'], 'jobsPerStatus': task['jobsPerStatus'],
                           'publication (f/t/D)': task['pubSummary']})

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
    instance = os.getenv('REST_Instance','preprod')
    work_dir = os.getenv('WORK_DIR','dummy_workdir')

    with open('%s/artifacts/submitted_tasks' %work_dir) as fp:
        tasks = fp.readlines()

    for task in tasks:
        remake_dict = {'task': task, 'instance': instance}
        remake_dir = crab_cmd({'cmd': 'remake', 'args': remake_dict})

        status_dict = {'dir': remake_dir}
        status_command_output = crab_cmd({'cmd': 'status', 'args': status_dict})
        status_command_output.update({'taskName': task.rstrip()})
        status_command_output['workdir'] = remake_dir
        listOfTasks.append(status_command_output)

    summary = parse_result(listOfTasks)

    with open('%s/artifacts/result' %work_dir, 'w') as fp:
        for result in summary:
            fp.write("%s\n" % result)

if __name__ == '__main__':
    main()
