#!/bin/bash
# NOTE: This file suppose to run inside cmssw sigularity (cc6,el7,el8)

echo "Starting taskSubmission.sh"
set -euo pipefail

echo "Verbose env.var. is set to $Verbose"
export Verbose
if [ $Verbose -eq 3 ]
then
  echo "enable bash trace"
  set -x
fi

#Script submits tasks for testing. 3 types of testing can be started using this script:
# 1. Client_Validation_Suite: tests different CRABClient commands;
# 2. Client_Configuration_Validation: tests different CRABClient configurations;
# 3. Task_Submission_Status_Tracking: checks if submitted tasks finished running.

# validate env var
# note: $PWD is (default to `./workdir`)
echo "(debug) ROOT_DIR=${ROOT_DIR}"
echo "(debug) WORK_DIR=${WORK_DIR}"
echo "(debug) X509_USER_PROXY=${X509_USER_PROXY}"
echo "(debug) REST_Instance=${REST_Instance}"
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
      [ $Verbose -ge 2 ] && cat "${file_name}"
      output=$(crab submit -c ${file_name} --proxy=${X509_USER_PROXY} 2>&1)
      submitExitCode=$?
      [ $Verbose -ge 1 ] && echo ${output}
      if [ $submitExitCode != 0 ]; then
          echo ${file_name} >> "${WORK_DIR}"/failed_tasks_${CI_PIPELINE_ID}_${CMSSW_release}
          killAfterFailure
      else
          echo ${output} | grep -oP '(?<=Task name:\s)[^\s]*(?=)' >> "${WORK_DIR}"/submitted_tasks_${2}
          echo ${output} | grep -oP '(?<=Project dir:\s)[^\s]*(?=)' >> "${WORK_DIR}"/project_directories
      fi
  done
}

immediateCheck(){
#Run immediate check on tasks submitted for Client_Configuration_Validation testing
#Save results to failed_CCV_tests and successfully_submitted_CCV_tests files

 echo "Runnng immediate checks on CCV tasks..."
 touch "${WORK_DIR}/successfully_submitted_CCV_tests"
 touch "${WORK_DIR}/failed_CCV_tests"
 for task in ${tasksToCheck};
 do
     echo -n "${task}: "
     test_to_execute=`echo "${task}" | grep -oP '(?<=_crab_).*(?=)'`
     task_dir=${WORK_DIR}/crab_${test_to_execute}
     if [ $Verbose -ge 1 ]; then
       trace="-x"
      else
        trace=""
      fi
     bash ${trace} ${test_to_execute}-testSubmit.sh ${task_dir} 2>&1 > testLog.txt && \
       (echo "${test_to_execute}-testSubmit.sh - ${task}" >> ${WORK_DIR}/successfully_submitted_CCV_tests; echo OK) || \
       (echo "${test_to_execute}-testSubmit.sh - ${task}" >> ${WORK_DIR}/failed_CCV_tests; echo FAIL; cat testLog.txt)
 done
}

killAfterFailure(){
#In case at least one task submission failed, kill all succesfully submitted tasks
 if [ -e "${WORK_DIR}/project_directories" ]; then
   echo -e "\nTask submission failed. Killing submitted tasks.\n"
   while read task ; do
       echo "Killing task: ${task}"
       crab kill --dir=${task} --proxy=${PROXY}
       killExitCode=$?
       if [ $killExitCode != 0 ]; then
           echo -e "Failed to kill: ${task}" >> ${WORK_DIR}/killed_tasks
       else

           echo -e "Successfully killed: ${task}" >> ${WORK_DIR}/killed_tasks
       fi
   done <${WORK_DIR}/project_directories
 fi
 exit 1
}

if [ "${Client_Validation_Suite}" = true ]; then
    echo -e "\nStarting task submission for Client Validation testing.\n"
    pushd "${ROOT_DIR}/test/clientValidationTasks/"
    filesToSubmit=`find . -type f -name '*.py' ! -name '*pset*'`
    submitTasks "${filesToSubmit}" "CV"
    cd ${WORK_DIR}
fi

if [ "${Client_Configuration_Validation}" = true ]; then
    echo -e "\nStarting task submission for Client Configuration Validation testing.\n"
    # mkdir -p tmpWorkDir
    # cd tmpWorkDir
    python3 ${ROOT_DIR}/test/makeTests.py
    filesToSubmit=`find . -maxdepth 1 -type f -name '*.py' ! -name '*PSET*'`
    submitTasks "${filesToSubmit}" "CCV"
    tasksToCheck=`cat ${WORK_DIR}/submitted_tasks_CCV`
    immediateCheck "${tasksToCheck}"
    # CCV tests, differently from others, have a validation step after submission, check it
    if [ -s ${WORK_DIR}/failed_CCV_tests ]; then
      echo "Some CCV tests failed immediate check after submission"
      echo "======================================================="
      echo "==== FAILED TESTS "
      cat ${WORK_DIR}/failed_CCV_tests
      echo "======================================================="
      exit 1
    fi

    cd ${WORK_DIR}
fi

if [ "${Task_Submission_Status_Tracking}" = true ]; then
    echo -e "\nStarting task submission for Status Tracking testing.\n"
    pushd "${ROOT_DIR}"/test/statusTrackingTasks/
    # for test
    if [[ -n ${DEBUG_TEST:-} ]]; then
        #filesToSubmit=$(find . -type f -name '*.py' ! -name '*pset*' | grep HC-splitByLumi.py)
        filesToSubmit=$(find . -type f -name '*.py' ! -name '*pset*' | grep Pythia.py)
    else
        filesToSubmit=$(find . -type f -name '*.py' ! -name '*pset*')
    fi
    submitTasks "${filesToSubmit}" "TS"
    cd ${WORK_DIR}
fi
