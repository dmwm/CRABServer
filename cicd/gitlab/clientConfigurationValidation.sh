#! /bin/bash
# This script takes task names from submitted_tasks file and executes tests on each task name.
# 3 files are produced
#  1. successful_tests: tests that returned exit code 0;
#  2. retry_tests: tests that returned exit code 2, meaning test should be retried again later;
#  3. failed_tests: tests that returned exit code not equal to 0 or 2, i.e. test failed.

echo "Running clientConfigurationValidation.sh"
set -euo pipefail

echo "Verbose env.var. is set to $Verbose"
export Verbose
if [ X$Verbose == "X3" ]
then
  echo "enable bash trace"
  set -x
fi

# validate env var
# note: $PWD is (default to `./workdir`)
echo "(debug) ROOT_DIR=${ROOT_DIR}"
echo "(debug) WORK_DIR=${WORK_DIR}"
echo "(debug) X509_USER_PROXY=${X509_USER_PROXY}"
export PROXY=$X509_USER_PROXY  # for use in validation scripts

# Setup CRABClient
source "${ROOT_DIR}/cicd/gitlab/setupCRABClient.sh"
# above command sets $CMSSW_RELEASE_BASE/src as cwd, use that to run crab commands for tests
# to keep $WORK_DiR clean with only the test results
python3 ${ROOT_DIR}/test/makeTests.py

# Ensure these 3 files exist, even if empty, to avoid spurious errors
touch ${WORK_DIR}/successful_tests
touch ${WORK_DIR}/failed_tests
touch ${WORK_DIR}/retry_tests


# If some tests are not OK, not Fail,  we will retry those and possibly create a new retry file, so move it aside
# (once logic is well debugged can simply do rm+touch, or force to empty via true > file or truncate -s 0 file)
mv ${WORK_DIR}/retry_tests ${WORK_DIR}/old_retry_tests
touch ${WORK_DIR}/retry_tests

if [ -f "${WORK_DIR}/submitted_tasks_CCV" ]; then
  while read task; do
    echo "Processing task: $task"
    # Extract the test name from the task
    test_to_execute=$(echo "${task}" | grep -oP '(?<=_crab_).*(?=)')
    [ $Verbose -ge 1 ] && echo "Test to execute: ${test_to_execute}"
    # check which file the task was listed in to tell if it was OK or Failed in previous iteration
    # make sure that command in each line always end with exit code = 0 to avoid early exit due to set -eou pipefail
    testOK=0; testFail=0
    grep -q ${test_to_execute}  ${WORK_DIR}/successful_tests && testOK=1 || true
    grep -q ${test_to_execute}  ${WORK_DIR}/failed_tests  && testFail=1 || true
    if [ $testOK -eq 1 ]; then
      [ $Verbose -ge 1 ] && echo "test is OK. Do not run again"
      continue
    elif [ $testFail -eq 1 ]; then
      [ $Verbose -ge 1 ] && echo "test failed in previous iteration. Do not run again"
      continue
    else
      [ $Verbose -ge 1 ] && echo "test not run yet or asked to be run again. Execute it"
    fi

    retVal=0
    source ${test_to_execute}-check.sh ${task} || retVal=$?  # capture non-0 exit code while avoiding early exit
    if [ $Verbose -ge 1 ]; then echo "Task check ended with exit code: $retVal"; fi

    if [ $retVal -eq 0 ]; then
      [ $Verbose -ge 1 ] && echo "check was successful"
      echo ${test_to_execute}-check.sh ${task} - $retVal >> ${WORK_DIR}/successful_tests
    elif [ $retVal -eq 2 ]; then
      [ $Verbose -ge 1 ] && echo "check must run again later"
      echo ${test_to_execute}-check.sh ${task} - $retVal >> ${WORK_DIR}/retry_tests
    else
      [ $Verbose -ge 1 ] && echo "check failed"
      echo ${test_to_execute}-check.sh ${task} - $retVal >> ${WORK_DIR}/failed_tests
    fi
  done <"${WORK_DIR}/submitted_tasks_CCV"
  # print summary
  echo "======================================================="
  echo "==== SUCCESSFUL TESTS "
  echo ""
  cat  ${WORK_DIR}/successful_tests | cut -d ' ' -f 2 || true
  if [ ! -s ${WORK_DIR}/successful_tests ]; then echo "--- None ---"; fi
  echo "======================================================="
  echo "==== TESTS TO BE RUN AGAIN "
  echo ""
  cat  ${WORK_DIR}/retry_tests 2>/dev/null | cut -d ' ' -f 2 || true
  if [ ! -s ${WORK_DIR}/retry_tests ]; then echo "--- None ---"; fi
  echo "======================================================="
  echo "==== FAILED TESTS "
  echo ""
  cat  ${WORK_DIR}/failed_tests | cut -d ' ' -f 2 || true
  if [ ! -s ${WORK_DIR}/failed_tests ]; then echo "--- None ---"; fi
  echo "======================================================="

  # the caller (CCV_config.sh) will check check which of *_tests_.... files is empty to decide
  # if CCV overall was OK, Failed, or needs to be run again

else
  echo "Error: ${WORK_DIR}/submitted_tasks_CCV is not a file"
  exit 1
fi

