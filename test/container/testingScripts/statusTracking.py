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

            if ('finished', total_jobs) in task['jobsPerStatus'].items():
                result = 'TestPassed'
            elif any(k in task['jobsPerStatus'] for k in ('failed', 'held')):
                result = 'TestFailed'
            else:
                result = 'TestRunning'
        elif task['dbStatus'] in ['HOLDING', 'QUEUED', 'NEW']:
            result = 'TestRunning'
        else:
            result = 'TestFailed'

        testResult.append({'TN': task['taskName'], 'testResult': result, 'dbStatus': task['dbStatus'], 
                           'combinedStatus': task['status'], 'jobsPerStatus': task['jobsPerStatus']})

    return testResult


def main():

    listOfTasks = []
    instance = os.getenv('REST_Instance','preprod')
    work_dir=os.getenv('WORK_DIR','dummy_workdir')

    with open('%s/artifacts/submitted_tasks' %work_dir) as fp:
        tasks = fp.readlines()

    for task in tasks:
        remake_dict = {'task': task, 'instance': instance}
        remake_dir = crab_cmd({'cmd': 'remake', 'args': remake_dict})

        status_dict = {'dir': remake_dir}
        status_command_output = crab_cmd({'cmd': 'status', 'args': status_dict})
        status_command_output.update({'taskName': task.rstrip()})
        listOfTasks.append(status_command_output)

    summary = parse_result(listOfTasks)

    with open('%s/artifacts/result' %work_dir, 'w') as fp:
        for result in summary:
            fp.write("%s\n" % result)

if __name__ == '__main__':
    main()
