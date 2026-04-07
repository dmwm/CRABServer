#!/bin/bash
# this script is called in gitlab pipeline to execute ClientValidation step
# it assumes to be run with PWD as the top of a CRABServer GH clone

echo "Starting CV_config.sh"

set -euo pipefail

echo "Verbose env.var. is set to $Verbose"
export Verbose
if [ $Verbose -eq 3 ]
then
  echo "enable bash trace"
  set -x
fi

# default values
export Client_Validation_Suite="${Client_Validation_Suite:-}"
export ROOT_DIR="${PWD}"

# note: make sure $X509_USER_PROXY is single file with correct permission, ready to consume without issuing with voms-proxy-init again
echo "(DEBUG) ROOT_DIR: ${ROOT_DIR}"
echo "(DEBUG) X509_USER_PROXY: ${X509_USER_PROXY}"
echo "(DEBUG) CRABClient_version: ${CRABClient_version}"
echo "(DEBUG) REST_Instance: ${REST_Instance}"
echo "(DEBUG) CMSSW_release: ${CMSSW_release}"
echo "(debug) Client_Validation_Suite=${Client_Validation_Suite}"

# always run inside ./workdir
export WORK_DIR="${ROOT_DIR}/workdir_${CI_PIPELINE_ID}_${CMSSW_release}"

if [ ! -d "$WORK_DIR" ]; then
  mkdir -p "$WORK_DIR"
  echo "(DEBUG) workdir was created as $WORK_DIR"
fi

pushd "${WORK_DIR}" > /dev/null
echo "(DEBUG) Current working directory: ${WORK_DIR}"

# Get configuration from CMSSW_release
CONFIG_LINE="$(grep "CMSSW_release=${CMSSW_release};" "${ROOT_DIR}"/test/testingConfigs)"
SCRAM_ARCH="$(echo "${CONFIG_LINE}" | tr ';' '\n' | grep SCRAM_ARCH | sed 's|SCRAM_ARCH=||')"
inputDataset="$(echo "${CONFIG_LINE}" | tr ';' '\n' | grep inputDataset | sed 's|inputDataset=||')"
# see https://github.com/dmwm/WMCore/issues/11051 for info about SCRAM_ARCH formatting
singularity="$(echo "${SCRAM_ARCH}" | cut -d"_" -f 1 | tail -c 2)"
export SCRAM_ARCH inputDataset singularity

# Start tests
chmod +x ${ROOT_DIR}/cicd/gitlab/clientValidation.sh
if [[ "$singularity" == "6" || "$singularity" == "7" || "$singularity" == "8" ]]; then
    echo "Starting singularity ${singularity} container."
    if [ "X${singularity}" == X6 ] ; then export TEST_LIST=SL6_TESTS; fi
    if [ "X${singularity}" == X7 ] ; then export TEST_LIST=FULL_TEST; fi
    if [ "X${singularity}" == X8 ] ; then export TEST_LIST=FULL_TEST; fi

    scramprefix=cc${singularity}
    if [ "X${singularity}" == X6 ]; then scramprefix=cc${singularity}; fi
    if [ "X${singularity}" == X7 ]; then scramprefix=cc${singularity}; fi
    if [ "X${singularity}" == X8 ]; then scramprefix=el${singularity}; fi

    CV_EC=0
    /cvmfs/cms.cern.ch/common/cmssw-${scramprefix} -- "${ROOT_DIR}/cicd/gitlab/clientValidation.sh" || CV_EC=$?
    echo "clientValidation.sh ended with exit code: $CV_EC"
else 
    echo "!!! I am not prepared to run for slc${singularity}."
    exit 1
fi

if [[ $CV_EC -eq 4 ]]; then
  echo "Task has not completed yet, wait"
  exit 4
fi

# Extract test results from the log
grep -B1 "TEST_RESULT: FAILED" CV_Summary.txt > result_failed || true
grep -B1 "TEST_RESULT: OK" CV_Summary.txt > result_OK || true

# Update issue with test results
TEST_RESULT='FAILED'
if [[ -s "result_failed" ]]; then
    echo -e "Some CRABClient commands failed. Find failed commands below:"
    echo -e "\`\`\``cat result_failed`\n\`\`\`"
    ERR=true
elif [[ -s "result_OK" ]]; then
    echo "All commands were executed successfully."
    TEST_RESULT='SUCCEEDED'
    ERR=false
else
    echo "Something went wrong. Investigate manually."
    ERR=true
fi 

# Prepare result message for reporting
echo -e "**Test:** Client validation\n\
**Result:** ${TEST_RESULT}\n\
**Finished at:** $(date '+%Y-%m-%d %H:%M:%S')\n\
**Test log:** ${CI_JOB_URL}\n"

# Exit with error if any
if [[ $ERR == true ]]; then
    exit 1
fi