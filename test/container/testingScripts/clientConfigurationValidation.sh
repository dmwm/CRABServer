#!/bin/bash

#Script takes task names from submitted_tasks file and executes tests on each task name. 
#3 files are produced:
# 1. successful_tests: tests that returned exit code 0;
# 2. retry_tests: tests that returned exit code 2, meaning test should be retried again later;
# 3. failed_tests: tests that returned exit code not equal to 0 or 2, i.e. test failed.
#Script is used in Jenkins job CRABServer_ClientConfigurationValidation.

#setup CRABClient
source setupCRABClient.sh
python /data/CRABTesting/testingScripts/repos/CRABServer/test/makeTests.py

while read task ; do
  echo "$task"
  test_to_execute=`echo "${task}" | grep -oP '(?<=_crab_).*(?=)'`
  bash ${test_to_execute}-check.sh ${task}

  retVal=$?
  if [ $retVal -eq 0 ]; then
        echo ${test_to_execute}-check.sh ${task} - $retVal >> /artifacts/successful_tests
  elif [ $retVal -eq 2 ]; then
        echo ${test_to_execute}-check.sh ${task} - $retVal >> /artifacts/retry_tests
  else
        echo ${test_to_execute}-check.sh ${task} - $retVal >> /artifacts/failed_tests
  fi
done </artifacts/submitted_tasks
