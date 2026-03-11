#! /bin/bash
set -euo pipefail

# default values
export Client_Configuration_Validation="${Client_Configuration_Validation:-}"

# note: make sure $X509_USER_PROXY is single file with correct permission, ready to consume without issuing with voms-proxy-init again
echo "(DEBUG) X509_USER_PROXY: ${X509_USER_PROXY}"
echo "(DEBUG) CRABClient_version: ${CRABClient_version}"
echo "(DEBUG) REST_Instance: ${REST_Instance}"
echo "(DEBUG) CMSSW_release: ${CMSSW_release}"
echo "(DEBUG) Client_Configuration_Validation: ${Client_Configuration_Validation}"

# always run inside ./workdir
export ROOT_DIR="${PWD}"
export WORK_DIR="${PWD}/workdir"
if [ ! -d "$WORK_DIR" ]; then
  mkdir -p "$WORK_DIR"
  echo "(DEBUG) workdir was recreated"
fi
pushd "${WORK_DIR}"

# Get configuration from CMSSW_release
CONFIG_LINE="$(grep "CMSSW_release=${CMSSW_release};" "${ROOT_DIR}"/test/testingConfigs)"
SCRAM_ARCH="$(echo "${CONFIG_LINE}" | tr ';' '\n' | grep SCRAM_ARCH | sed 's|SCRAM_ARCH=||')"
inputDataset="$(echo "${CONFIG_LINE}" | tr ';' '\n' | grep inputDataset | sed 's|inputDataset=||')"
# see https://github.com/dmwm/WMCore/issues/11051 for info about SCRAM_ARCH formatting
singularity="$(echo "${SCRAM_ARCH}" | cut -d"_" -f 1 | tail -c 2)"
export SCRAM_ARCH inputDataset singularity

chmod +x ${ROOT_DIR}/cicd/gitlab/clientConfigurationValidation.sh
if [ "X${singularity}" == X6 ] || [ "X${singularity}" == X7 ] || [ "X${singularity}" == X8 ]; then
    echo "Starting singularity ${singularity} container."
    if [ "X${singularity}" == X6 ]; then scramprefix=cc${singularity}; fi
    if [ "X${singularity}" == X7 ]; then scramprefix=el${singularity}; fi
    if [ "X${singularity}" == X8 ]; then scramprefix=el${singularity}; fi
    ERR=false;
    /cvmfs/cms.cern.ch/common/cmssw-${scramprefix} -- "${ROOT_DIR}/cicd/gitlab/clientConfigurationValidation.sh"
    CCV_EC=$?
    echo "clientConfigurationValidation.sh script ended wih exit code $CCV_EC"
    ERR=$([[ $CCV_EC -eq 0 || $CCV_EC -eq 2 ]] && echo "false" || echo "true") #ERR is true if return is other than 0 or 2
    Check=
else
    echo "!!! I am not prepared to run for slc${singularity}."
    exit 1
fi

if [ "$ERR" == true ]; then
    echo "clientConfigurationValidation.sh script failed to run properly."
    exit 1
fi


# Check if the tests passed or failed. Logic is:
# if some tests needs retrying, run again CCV script (will skip tests already done)
# when all tests have run (no retry) if at least one test failed, CCV failed
# otherwise CCV is successful

if [ -s "retry_tests_${CI_PIPELINE_ID}_${CMSSW_release}" ]; then
    # file size is > 0
    TEST_RESULT='FULL-STATUS-UNKNOWN'  # means: wait and then run tests again
else
  # no more retries are needed
  if [ -s "failed_tests_${CI_PIPELINE_ID}_${CMSSW_release}" ]; then
    # file not empty, some tests failed
    TEST_RESULT='FAILED'
  elif [ -s "successful_tests_${CI_PIPELINE_ID}_${CMSSW_release}" ]; then
    # sanity check... this file better be not empty now !
    TEST_RESULT='SUCCEEDED'
  else
    echo "Test summary files are all empty !! ?? Things went REALLY BAD. Please investigate"
    exit 1
  fi
fi

# Write out the final result message
echo -e "**Test:** Client configuration validation\n\
**Result:** ${TEST_RESULT}\n\
**Finished at:** $(date '+%Y-%m-%d %H:%M:%S %Z')\n\
**Test log:** ${CI_JOB_URL}\n" > message_CCVResult

cat message_CCVResult

popd > /dev/null

echo "CCV test iteration completed with status: ${TEST_RESULT}"

if [[ ${TEST_RESULT} == 'FULL-STATUS-UNKNOWN' ]]; then
    exit 4
elif [[ ${TEST_RESULT} == 'SUCCEEDED' ]]; then
    exit 0
elif [[ ${TEST_RESULT} == 'FAILED' ]]; then
    exit 1
fi
