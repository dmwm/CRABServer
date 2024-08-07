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

#setup CRABClient
source "${ROOT_DIR}"/cicd/gitlab/setupCRABClient.sh
python ${WORK_DIR}/CRABServer/test/makeTests.py

while read task ; do
  echo "$task"
  test_to_execute=`echo "${task}" | grep -oP '(?<=_crab_).*(?=)'`
  bash -x ${test_to_execute}-check.sh ${task}

  retVal=$?
  if [ $retVal -eq 0 ]; then
        echo ${test_to_execute}-check.sh ${task} - $retVal >> ${WORK_DIR}/artifacts/successful_tests
  elif [ $retVal -eq 2 ]; then
        echo ${test_to_execute}-check.sh ${task} - $retVal >> ${WORK_DIR}/artifacts/retry_tests
  else
        echo ${test_to_execute}-check.sh ${task} - $retVal >> ${WORK_DIR}/artifacts/failed_tests
  fi
done <${WORK_DIR}/artifacts/submitted_tasks
