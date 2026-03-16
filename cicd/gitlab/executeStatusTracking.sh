#! /bin/bash

echo "Running executeStatusTracking.sh"

set -euo pipefail

echo "Verbose env.var. is set to $Verbose"
export Verbose
if [ X$Verbose == "X3" ]
then
  echo "enable bash trace"
  set -x
fi

# check parameters
echo "(DEBUG) X509_USER_PROXY=${X509_USER_PROXY}"
echo "(DEBUG) REST_Instance=${REST_Instance}"
echo "(DEBUG) CMSSW_release=${CMSSW_release}"
echo "(DEBUG) SCRAM_ARCH=${SCRAM_ARCH}"
echo "(DEBUG) Check_Publication_Status=${Check_Publication_Status}"
echo "(DEBUG) CRABClient_version=${CRABClient_version}"

#0. Prepare environment
export ROOT_DIR="${PWD}"
export WORK_DIR="${PWD}/workdir_${CI_PIPELINE_ID}_${CMSSW_release}"
if [ ! -d "$WORK_DIR" ]; then
  mkdir -p "$WORK_DIR"
fi
pushd "${WORK_DIR}"

if [ $RETRY -eq 1 ]; then
  echo -e "\nWill check status of tasks:"
  cat submitted_tasks_TS
  echo -e "\n"
fi

#1.2. Run tests.
# use CMSSW_15 and python3 to run CCV tests, those tests do not depend on
# what release was used for submitting
CMSSW_release=CMSSW_15_0_18
SCRAM_ARCH=el8_amd64_gcc12
scramprefix=el8

ERR=false
/cvmfs/cms.cern.ch/common/cmssw-${scramprefix} -- bash  "${ROOT_DIR}"/cicd/gitlab/st/statusTracking.sh || ERR=true

if [ "$ERR" == true ]; then
    echo "statusTracking.sh script failed to run properly."
    exit 1
fi

#3. Print summary

python3 ${ROOT_DIR}"/cicd/gitlab/st/printStatusTrackingSummary.py"

#4. Determine final status and decide if OK/Retry/FAIL
TEST_RESULT='FAILED'
if [ ! -s "./result" ]; then
	MESSAGE='Result file is empty. Something went wrong. Investigate manually.'
elif grep -q -E "(SUBMITFAILED|SUBMITREFUSED)" ./result; then
    MESSAGE='Task(s) has "SUBMITFAILED" status. Investigate manually.'
elif grep -q "TestFailed" ./result ; then
	MESSAGE='Test failed. Investigate manually'
elif grep -q "TestRunning" result || grep -q "TestResubmitted" ./result; then
    MESSAGE='Will run again.'
   	TEST_RESULT='FULL-STATUS-UNKNOWN'
else
	MESSAGE='Test is done.'
   	TEST_RESULT='SUCCEEDED'
fi

echo "======================================================"
echo -e """**Test:** Task Submission Status Tracking\n\
**Result:** ${TEST_RESULT}\n\
**Attempt:** ${RETRY} out of ${RETRY_MAX}. ${MESSAGE}\n\
**Finished at:** `(date '+%Y-%m-%d %H:%M:%S %Z')`\n\
"""

popd

if [[ ${TEST_RESULT} == 'FULL-STATUS-UNKNOWN' ]]; then
    exit 4
elif [[ ${TEST_RESULT} == 'SUCCEEDED' ]]; then
    exit 0
elif [[ ${TEST_RESULT} == 'FAILED' ]]; then
    exit 1
fi
