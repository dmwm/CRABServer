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

mkdir -p "${WORK_DIR}/artifacts"
cat <<EOF > "${WORK_DIR}/artifacts/submitted_tasks_CCV"
240904_190936:cmsbot_crab_transferOutputs
240904_190939:cmsbot_crab_transferLogs
240904_190941:cmsbot_crab_activity
240904_190944:cmsbot_crab_inputFiles
240904_190947:cmsbot_crab_disableAutomaticOutputCollection
240904_190949:cmsbot_crab_outputFiles
240904_190952:cmsbot_crab_allowUndistributedCMSSW
240904_190954:cmsbot_crab_maxMemoryMB
240904_190957:cmsbot_crab_maxJobRuntimeMin
240904_190959:cmsbot_crab_numCores
240904_191002:cmsbot_crab_scriptExe
240904_191004:cmsbot_crab_scriptArgs
240904_191007:cmsbot_crab_sendVenvFolder
240904_191010:cmsbot_crab_sendExternalFolder
240904_191012:cmsbot_crab_inputDBS
240904_191015:cmsbot_crab_useParent
240904_191017:cmsbot_crab_secondaryInputDataset
240904_191020:cmsbot_crab_lumiMaskFile
240904_191023:cmsbot_crab_lumiMaskUrl
240904_191026:cmsbot_crab_outLFNDirBase
240904_191029:cmsbot_crab_runRange
240904_191031:cmsbot_crab_ignoreLocality
240904_191034:cmsbot_crab_userInputFiles
240904_191036:cmsbot_crab_whitelist
240904_191039:cmsbot_crab_blacklist
240904_191041:cmsbot_crab_ignoreGlobalBlacklist
240904_191044:cmsbot_crab_voRole
240904_191046:cmsbot_crab_voGroup
240904_191048:cmsbot_crab_scheddName
240904_191051:cmsbot_crab_collector
240904_191053:cmsbot_crab_extraJDL
EOF


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
          #killAfterFailure
      else
          echo ${output} | grep -oP '(?<=Task name:\s)[^\s]*(?=)' >> "${WORK_DIR}"/artifacts/submitted_tasks_${2}
          echo ${output} | grep -oP '(?<=Project dir:\s)[^\s]*(?=)' >> "${WORK_DIR}"/project_directories
      fi
  done
}

# killAfterFailure(){
# #In case at least one task submission failed, kill all succesfully submitted tasks
#  if [ -e "${WORK_DIR}/artifacts/project_directories" ]; then
#    echo -e "\nTask submission failed. Killing submitted tasks.\n"
#    while read task ; do
#        echo "Killing task: ${task}"
#        crab kill --dir=${task} --proxy=${PROXY}
#        killExitCode=$?
#        if [ $killExitCode != 0 ]; then
#            echo -e "Failed to kill: ${task}" >> ${WORK_DIR}/artifacts/killed_tasks
#        else
#            echo -e "Successfully killed: ${task}" >> ${WORK_DIR}/artifacts/killed_tasks
#        fi
#    done <${WORK_DIR}/artifacts/project_directories
#  fi
#  exit 1
# }

immediateCheck(){
#Run immediate check on tasks submitted for Client_Configuration_Validation testing
#Save results to failed_tests and successful_tests files
 for task in ${tasksToCheck};
 do
     echo -e "\nRunning immediate test on task: ${task}"
     test_to_execute=`echo "${task}" | grep -oP '(?<=_crab_).*(?=)'`
     task_dir=${WORK_DIR}/crab_${test_to_execute}
     bash -x ${test_to_execute}-testSubmit.sh ${task_dir} && \
       echo ${test_to_execute}-testSubmit.sh ${task_dir} - $? >> ${WORK_DIR}/successful_tests || \
       echo ${test_to_execute}-testSubmit.sh ${task_dir} - $? >> ${WORK_DIR}/failed_tests
 done
}

Client_Validation_Suite=${Client_Validation_Suite:-}
Client_Configuration_Validation=${Client_Configuration_Validation:-}
Task_Submission_Status_Tracking=${Task_Submission_Status_Tracking:-}

# TODO: need to run makeTests.py outside apptainer because python3 is not available in slc6
if [ "${Client_Configuration_Validation}" = true ]; then
   echo -e "\nStarting task submission for Client Configuration Validation testing.\n"
   tmpWorkDir="${WORK_DIR}/submitted_tasks_CCV"
   rm -rf "$tmpWorkDir"
   mkdir -p "$tmpWorkDir"
   cd "$tmpWorkDir"
   python3 "${ROOT_DIR}/test/makeTests.py"
   filesToSubmit=`find . -maxdepth 1 -type f -name '*.py' ! -name '*PSET*'`
   submitTasks "${filesToSubmit}" "CCV"
   tasksToCheck=`cat ${WORK_DIR}/artifacts/submitted_tasks_CCV`
   immediateCheck "${tasksToCheck}"
   cd ${WORK_DIR}
fi
