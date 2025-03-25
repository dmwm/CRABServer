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
    ERR=$([[ $? -eq 0 || $? -eq 2 ]] && echo "false" || echo "true") #ERR is true if return is other than 0 or 2
else
    echo "!!! I am not prepared to run for slc${singularity}."
    exit 1
fi

if [ "$ERR" == true ]; then
    echo "clientConfigurationValidation.sh script failed to run properly."
    exit 1
fi
# Change to the working directory
# cd ${WORK_DIR}

# Set retry variables (GitLab does not have NAGINATOR; you can set these manually or use GitLab CI variables)
export RETRY=${CI_PIPELINE_RETRY_COUNT:-0}
export MAX_RETRY=${CI_PIPELINE_MAX_RETRIES:-4}

# 2. test results
TEST_RESULT='FAILED'
MESSAGE='Test failed. Investigate manually'

# Define an associative array to hold the test results
declare -A results=( ["SUCCESSFUL"]="successful_tests_${CI_PIPELINE_ID}" ["FAILED"]="failed_tests_${CI_PIPELINE_ID}" ["RETRY"]="retry_tests_${CI_PIPELINE_ID}" )

for result in "${!results[@]}"; do
	if [ -s "${results[$result]}" ]; then
		test_result=$(cat "${results[$result]}")
		echo -e "\n${result} TESTS:\n${test_result}" >> message_CCVResult_interim

		# Handle retry logic
		if [ "${results[$result]}" == "retry_tests_${CI_PIPELINE_ID}" ]; then
            TEST_RESULT='FULL-STATUS-UNKNOWN'
            if [ "$RETRY" -ge "$MAX_RETRY" ]; then
                MESSAGE='Exceeded configured retries. If needed restart manually.'
            else
                MESSAGE='Will run again.'
            fi
        fi
	else
		echo -e "\n${result} TESTS:\n none" >> message_CCVResult_interim
	fi
done

# Check if the tests passed or failed
if [ -s "failed_tests_${CI_PIPELINE_ID}" ]; then
    TEST_RESULT='FAILED'
elif [ -s "retry_tests_${CI_PIPELINE_ID}" ]; then
    TEST_RESULT='FULL-STATUS-UNKNOWN'
elif [ -s "successful_tests_${CI_PIPELINE_ID}" ]; then
    TEST_RESULT='SUCCEEDED'
    MESSAGE='Test is done.'
fi

# Create the final result message
echo -e "**Test:** Client configuration validation\n\
**Result:** ${TEST_RESULT}\n\
**Attempt:** ${RETRY} out of ${MAX_RETRY}. ${MESSAGE}\n\
**Finished at:** $(date '+%Y-%m-%d %H:%M:%S')\n\
**Test log:** ${CI_JOB_URL}\n" > message_CCVResult

# Append interim result to the final message
echo -e "\`\`\`$(cat message_CCVResult_interim)\n\`\`\`"  >> message_CCVResult

popd

if [[ ${TEST_RESULT} == 'FULL-STATUS-UNKNOWN' ]]; then
    exit 4
elif [[ ${TEST_RESULT} == 'SUCCEEDED' ]]; then
    exit 0
elif [[ ${TEST_RESULT} == 'FAILED' ]]; then
    exit 1
fi
