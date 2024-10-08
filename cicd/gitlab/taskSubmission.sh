#!/bin/bash
# NOTE: This file suppose to run inside cmssw sigularity (cc6,el7,el8)

set -x
set -euo pipefail

#Script submits tasks for testing. 3 types of testing can be started using this script:
# 1. Client_Validation_Suite: tests different CRABClient commands;
# 2. Client_Configuration_Validation: tests different CRABClient configurations;
# 3. Task_Submission_Status_Tracking: checks if submitted tasks finished running.
#Variabiles ${REST_Instance}, ${Client_Validation_Suite}, ${Client_Configuration_Validation} and ${Task_Submission_Status_Tracking} comes from
#Jenkins job CRABServer_ExecuteTests configuration.

# validate env var
# note: $PWD is (default to `./workdir`)
echo "(debug) ROOT_DIR=${ROOT_DIR}"
echo "(debug) WORK_DIR=${WORK_DIR}"
echo "(debug) X509_USER_PROXY=${X509_USER_PROXY}"
echo "(debug) Client_Validation_Suite=${Client_Validation_Suite}"
echo "(debug) Client_Configuration_Validation=${Client_Configuration_Validation}"
echo "(debug) Task_Submission_Status_Tracking=${Task_Submission_Status_Tracking}"

# init crabclient
source "${ROOT_DIR}"/cicd/gitlab/setupCRABClient.sh

submitTasks() {
#Submit tasks and based on the exit code add task name / file name respectively to submitted_tasks or failed_tasks file

  for file_name in ${filesToSubmit};
  do
      echo -e "\nSubmitting file: ${file_name}"
      cat "${file_name}"
      output=$(crab submit -c ${file_name} --proxy=${X509_USER_PROXY} 2>&1)
      submitExitCode=$?
      echo ${output}
      if [ $submitExitCode != 0 ]; then
          echo ${file_name} >> "${WORK_DIR}"/failed_tasks
          killAfterFailure
      else
          echo ${output} | grep -oP '(?<=Task name:\s)[^\s]*(?=)' >> "${WORK_DIR}"/submitted_tasks_${2}
          echo ${output} | grep -oP '(?<=Project dir:\s)[^\s]*(?=)' >> "${WORK_DIR}"/project_directories
      fi
  done
}

killAfterFailure(){
#In case at least one task submission failed, kill all succesfully submitted tasks
 if [ -e "${WORK_DIR}/artifacts/project_directories" ]; then
   echo -e "\nTask submission failed. Killing submitted tasks.\n"
   while read task ; do
       echo "Killing task: ${task}"
       crab kill --dir=${task} --proxy=${PROXY}
       killExitCode=$?
       if [ $killExitCode != 0 ]; then
           echo -e "Failed to kill: ${task}" >> ${WORK_DIR}/artifacts/killed_tasks
       else
           echo -e "Successfully killed: ${task}" >> ${WORK_DIR}/artifacts/killed_tasks
       fi
   done <${WORK_DIR}/artifacts/project_directories
 fi
 exit 1
}

if [ "${Task_Submission_Status_Tracking}" = true ]; then
    echo -e "\nStarting task submission for Status Tracking testing.\n"
    pushd "${ROOT_DIR}"/test/statusTrackingTasks/
    # for test
    if [[ -n ${DEBUG_TEST:-} ]]; then
        filesToSubmit=$(find . -type f -name '*.py' ! -name '*pset*' | grep HC-splitByLumi.py)
    else
        filesToSubmit=$(find . -type f -name '*.py' ! -name '*pset*')
    fi
    submitTasks "${filesToSubmit}" "TS"
    popd
fi
