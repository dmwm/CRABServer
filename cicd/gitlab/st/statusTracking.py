#! /usr/bin/env python

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
        return None
    except ClientException as cle:
        print('Failed', configuration['cmd'], 'of the task: %s' % (cle))
        return None


def parse_result(listOfTasks, checkPublication=False):

    testResult = []

    for task in listOfTasks:
        task['pubSummary'] = 'None'  # make sure this is initialized
        needToResubmit = False
        needToResubmitPublication = False
        automaticSubmit = '0-1' in task['jobs'].keys()  # if there are probe jobs
        if task['dbStatus'] == 'SUBMITTED':
            if automaticSubmit:
                handle_automatic_split_jobs(task)
            probe_jobs = task['jobsPerStatus']['probe'] if 'probe' in task['jobsPerStatus'] else 0
            rescheduled_jobs = task['jobsPerStatus']['rescheduled'] if 'rescheduled' in task['jobsPerStatus'] else 0
            total_jobs = sum(task['jobsPerStatus'].values()) - probe_jobs - rescheduled_jobs
            finished_jobs = task['jobsPerStatus']['finished'] if 'finished' in task['jobsPerStatus'] else 0
            print(finished_jobs)
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
                published_in_dbs = int(ret.decode().strip()) if isinstance(ret, bytes) else int(ret.strip()) # dirty trick from b'999\n' to 999 (e.g.)
            else:
                published_in_dbs = 0

            task['pubSummary'] = '%d/%d/%d' % (failedPublications, published_in_transfersdb, published_in_dbs)

            if total_jobs > 0 and ('finished', total_jobs) in task['jobsPerStatus'].items():
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
                        #result = 'TestFailed'
                        needToResubmitPublication = True
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
        if needToResubmit and not automaticSubmit:
            resubmit = crab_cmd({'cmd': 'resubmit', 'args': {'dir': task['workdir']}})
            result = 'TestResubmitted'
            print(resubmit)
        if needToResubmitPublication:
            resubmit = crab_cmd({'cmd': 'resubmit', 'args': {'dir': task['workdir'], 'publication': True}})
            result = 'TestResubmitted'
            print(resubmit)

        testResult.append({'TN': task['taskName'], 'testResult': result, 'dbStatus': task['dbStatus'],
                           'combinedStatus': task['status'], 'jobsPerStatus': task['jobsPerStatus'],
                           'publication (fail/tdb/DBS)': task['pubSummary']})

    return testResult


def  handle_automatic_split_jobs(task):
    """ modify accounting in task dictionary to deal with automatic splitting probes and tails """
    task['jobsPerStatus']['probe'] = 0  # prepare a new counter
    task['jobsPerStatus']['rescheduled'] = 0  # prepare a new counter
    for job in task['jobs'].keys():
        if '0-' in job:
            # ignore completed probe jobs, OK or not
            task['jobsPerStatus']['probe'] += 1
            if task['jobs'][job]['State'] == 'finished':
                task['jobsPerStatus']['finished'] -= 1
            if task['jobs'][job]['State'] == 'failed':
                task['jobsPerStatus']['failed'] -= 1
            task['jobs'][job]['State'] = 'probe'
        if not '-' in job and task['jobs'][job]['State'] == 'failed':
            task['jobs'][job]['State'] = 'rescheduled'  # failed processing jobs will run as tail
            task['jobsPerStatus']['failed'] -= 1        # ignore their failures
            task['jobsPerStatus']['rescheduled'] +=1

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
    import json
    print(json.dumps(dict(os.environ), indent=4))
    listOfTasks = []
    instance = os.getenv('REST_Instance', 'preprod')
    work_dir = os.getenv('WORK_DIR', 'dummy_workdir')
    Check_Publication_Status = os.getenv('Check_Publication_Status', 'No')
    print("Check_Publication_Status is : ", Check_Publication_Status )
    checkPublication = True if Check_Publication_Status == 'Yes' else False

    # Read all tasks from the specified files into a single list
    tasks = [
    line 
    for file_name in ['submitted_tasks_TS', 'submitted_tasks_CCV', 'submitted_tasks_CV']
    for line in open(f'{work_dir}/{file_name}').readlines()
    ]


    for task in tasks:
        # when testing it helps to reuse already made directories
        remake_dir = '_'.join(task.rstrip().split('_')[-3:])
        if not os.path.isdir(remake_dir):
            remake_dict = {'task': task, 'instance': instance}
            outputDict = crab_cmd({'cmd': 'remake', 'args': remake_dict})
            remake_dir = outputDict['workDir']

        status_dict = {'dir': remake_dir}
        status_command_output = crab_cmd({'cmd': 'status', 'args': status_dict})
        if status_command_output:
            status_command_output.update({'taskName': task.rstrip()})
            status_command_output['workdir'] = remake_dir
            listOfTasks.append(status_command_output)
        else:
            print("status_command_output is None for", task)

    summary = parse_result(listOfTasks,checkPublication)

    with open('%s/result' %work_dir, 'w') as fp:
        for result in summary:
            fp.write("%s\n" % result)

if __name__ == '__main__':
    main()
