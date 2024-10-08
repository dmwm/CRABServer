#! /bin/bash

set -euo pipefail

# check parameters
echo "(DEBUG) X509_USER_PROXY=${X509_USER_PROXY}"
echo "(DEBUG) REST_Instance=${REST_Instance}"
echo "(DEBUG) CMSSW_release=${CMSSW_release}"
echo "(DEBUG) SCRAM_ARCH=${SCRAM_ARCH}"
echo "(DEBUG) Check_Publication_Status=${Check_Publication_Status}"
echo "(DEBUG) CRABClient_version=${CRABClient_version}"

#0. Prepare environment
export ROOT_DIR="${PWD}"
export WORK_DIR="${PWD}/workdir"
if [ ! -d "$WORK_DIR" ]; then
  mkdir -p "$WORK_DIR"
fi
pushd "${WORK_DIR}"

#1.2. Run tests
singularity=$(echo ${SCRAM_ARCH} | cut -d"_" -f 1 | tail -c 2)
scramprefix=cc${singularity}
if [ "X${singularity}" == X6 ]; then scramprefix=cc${singularity}; fi
if [ "X${singularity}" == X8 ]; then scramprefix=el${singularity}; fi

ERR=false
/cvmfs/cms.cern.ch/common/cmssw-${scramprefix} -- bash -x "${ROOT_DIR}"/cicd/gitlab/st/statusTracking.sh || ERR=true

#3. Update issue with submission results
TEST_RESULT='FAILED'
if [ ! -s "./result" ]; then
	MESSAGE='Something went wrong. Investigate manually.'
   	ERR=true
elif grep -E "(SUBMITFAILED|SUBMITREFUSED)" ./result; then
    MESSAGE='Task(s) has "SUBMITFAILED" status. Investigate manually.'
   	ERR=true
elif grep "TestRunning" result || grep "TestResubmitted" result; then
    MESSAGE='Will run again.'
   	TEST_RESULT='FULL-STATUS-UNKNOWN'
elif grep "TestFailed" result ; then
	MESSAGE='Test failed. Investigate manually'
    ERR=true
else
	MESSAGE='Test is done.'
   	TEST_RESULT='SUCCEEDED'
fi

echo -e """**Test:** Task Submission Status Tracking\n\
**Result:** ${TEST_RESULT}\n\
**Attempt:** ${RETRY} out of ${RETRY_MAX}. ${MESSAGE}\n\
**Finished at:** `(date '+%Y-%m-%d %H:%M:%S %Z')`\n\
"""

echo -e "\`\`\`\n`cat result`\n\`\`\`" || true

popd

if $ERR ; then
	exit 1
fi
if [[ ${TEST_RESULT} == 'FULL-STATUS-UNKNOWN' ]]; then
    exit 4
fi
