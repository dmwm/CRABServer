#!/bin/bash
set -euo pipefail

# default values
export Client_Validation_Suite="${Client_Validation_Suite:-}"

# note: make sure $X509_USER_PROXY is single file with correct permission, ready to consume without issuing with voms-proxy-init again
echo "(DEBUG) X509_USER_PROXY: ${X509_USER_PROXY}"
echo "(DEBUG) CRABClient_version: ${CRABClient_version}"
echo "(DEBUG) REST_Instance: ${REST_Instance}"
echo "(DEBUG) CMSSW_release: ${CMSSW_release}"
echo "(debug) Client_Validation_Suite=${Client_Validation_Suite}"

# always run inside ./workdir
export ROOT_DIR=$PWD
export WORK_DIR=$PWD/workdir
mkdir -p workdir
pushd "${WORK_DIR}"

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
    
    /cvmfs/cms.cern.ch/common/cmssw-${scramprefix} -- "${ROOT_DIR}/cicd/gitlab/clientValidation.sh" || ERR=true
    
else 
    echo "!!! I am not prepared to run for slc${singularity}."
    exit 1
fi

# Extract test results from the log
awk -v RS="____________" '/TEST_RESULT:\sFAILED/' client-validation.log > result_failed
awk -v RS="____________" '/TEST_RESULT:\sOK/' client-validation.log > result_OK

# Update issue with test results
TEST_RESULT='FAILED'
if [[ -s "result_failed" ]]; then
    echo -e "Some CRABClient commands failed. Find failed commands below:" >> message_CVResult_interim
    echo -e "\`\`\``cat result_failed`\n\`\`\`" >> message_CVResult_interim
    ERR=true
elif [[ -s "result_OK" ]]; then
    echo "All commands were executed successfully." >> message_CVResult_interim
    TEST_RESULT='SUCCEEDED'
else
    echo "Something went wrong. Investigate manually." >> message_CVResult_interim
    ERR=true
fi 

# Prepare result message for reporting
echo -e "**Test:** Client validation\n\
**Result:** ${TEST_RESULT}\n\
**Finished at:** $(date '+%Y-%m-%d %H:%M:%S')\n\
**Test log:** ${CI_JOB_URL}\n\
**Message:** $(cat message_CVResult_interim)\n" > message_CVResult

# Exit with error if any
if [[ $ERR == true ]]; then
    exit 1
fi