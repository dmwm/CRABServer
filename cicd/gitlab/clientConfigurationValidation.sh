set -x
set -euo pipefail

# validate env var
# note: $PWD is (default to `./workdir`)
echo "(debug) ROOT_DIR=${ROOT_DIR}"
echo "(debug) WORK_DIR=${WORK_DIR}"
echo "(debug) X509_USER_PROXY=${X509_USER_PROXY}"

#Script takes task names from submitted_tasks file and executes tests on each task name. 
#3 files are produced:
# 1. successful_tests: tests that returned exit code 0;
# 2. retry_tests: tests that returned exit code 2, meaning test should be retried again later;
# 3. failed_tests: tests that returned exit code not equal to 0 or 2, i.e. test failed.
#Script is used in Jenkins job CRABServer_ClientConfigurationValidation.

# Setup CRABClient
source "${ROOT_DIR}/cicd/gitlab/setupCRABClient.sh"
python3 ${ROOT_DIR}/test/makeTests.py

# Ensure the log files exist (creating successful_tests (if doesn't exist) and deleting/recreating retry_tests and failed_tests)
touch ${WORK_DIR}/successful_tests_${CI_PIPELINE_ID}_${CMSSW_release}

# Delete and recreate retry_tests and failed_tests (if they exist)
rm -f ${WORK_DIR}/retry_tests ${WORK_DIR}/failed_tests_${CI_PIPELINE_ID}_${CMSSW_release}
touch ${WORK_DIR}/retry_tests ${WORK_DIR}/failed_tests_${CI_PIPELINE_ID}_${CMSSW_release}

if [ -f "${WORK_DIR}/submitted_tasks_CCV_${CI_PIPELINE_ID}_${CMSSW_release}" ]; then
  while read task; do
    echo "Processing task: $task"

    # Extract the test name from the task
    test_to_execute=$(echo "${task}" | grep -oP '(?<=_crab_).*(?=)')
    echo "Test to execute: ${test_to_execute}"

    bash -x ${test_to_execute}-check.sh ${task}
    retVal=$?
    echo "Exit Code: $retVal"

    if [ $retVal -eq 0 ]; then
      echo ${test_to_execute}-check.sh ${task} - $retVal >> ${WORK_DIR}/successful_tests_${CI_PIPELINE_ID}_${CMSSW_release}
    elif [ $retVal -eq 2 ]; then
      echo ${test_to_execute}-check.sh ${task} - $retVal >> ${WORK_DIR}/retry_tests_${CI_PIPELINE_ID}_${CMSSW_release}
    else
      echo ${test_to_execute}-check.sh ${task} - $retVal >> ${WORK_DIR}/failed_tests_${CI_PIPELINE_ID}_${CMSSW_release}
    fi
  done <"${WORK_DIR}/submitted_tasks_CCV_${CI_PIPELINE_ID}_${CMSSW_release}"
else
  echo "Error: ${WORK_DIR}/submitted_tasks_CCV_${CI_PIPELINE_ID}_${CMSSW_release} is not a file"
  exit 1
fi

