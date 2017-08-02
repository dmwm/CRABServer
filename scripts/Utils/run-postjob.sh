#!/bin/bash

if [[ "$0" != "/bin/bash" && "$0" != "-bash" && "$0" != "bash" ]] ; then
    echo "BEWARE: THIS SCRIPT NEEDS TO BE source'D NOT EXECUTED"
    exit
fi

#set -x

if [ $# -ne 2 ]
then
  echo "usage: $0 <task_name> <jobId>"
  echo " will re-run postjob for that job on that task "
  return
fi

# start from task name
taskName=$1
# and a job number (id in task) for which to rerun PostJob
jobId=$2
# and for which retry number
jobRetry=0

#
#========== Now the real action
# see https://twiki.cern.ch/twiki/bin/view/CMSPublic/Crab3OperatorDebugging#To_run_a_post_job_on_the_schedd
#
echo "INFO: Will get ready to rerun PostJob for jobId $jobId, retry #${jobRetry} of task ${taskName}"
echo "INFO:   Beware error checking in this script is minimal"

export _CONDOR_PER_JOB_HISTORY_DIR=`condor_config_val PER_JOB_HISTORY_DIR`

# find the task spool directory
constrain=\'crab_workflow==\"$taskName\"\&\&jobUniverse==7\'
cat > /tmp/myCondorQ.sh <<EOF
condor_q -con $constrain -af iwd
EOF
chmod +x /tmp/myCondorQ.sh
spoolDir=`/tmp/myCondorQ.sh`
echo "INFO: Task SPOOL_DIR: " $spoolDir
rm /tmp/myCondorQ.sh

#
cd $spoolDir

# find who owns the spool directory
username=`ls -ld ${PWD}|awk '{print $3}'`

# current username is $USER, make sure it matches
if [ $USER != $username ]
then
  echo "ERROR: need to have access to task SPOOL  directory. Do:"
  echo "  sudo su $username"
  echo "and then source this script again"
  return
fi

# grab the proxy
export X509_USER_PROXY=`ls|egrep  [a-z,0-9]{40}`
#voms-proxy-info

# grab args from the dagman config
PJargs=`cat RunJobs*.*dag | grep "POST Job${jobId}" | awk 'BEGIN { FS="MAX_RETRIES" }; {print $2}'`

# find the condor clusterId for the job 
jobClusterId=`ls -l finished_jobs/job.${jobId}.${jobRetry} | awk '{print $NF}'|cut -d. -f2-3`

# reset PJ count
echo '{"pre": 1, "post": 0}' > retry_info/job.${jobId}.txt
rm defer_info/defer_num.${jobId}.0.txt

# these two are mandatory
export _CONDOR_JOB_AD=finished_jobs/job.${jobId}.0
export TEST_DONT_REDIRECT_STDOUT=True

# to disable the job retry handling part
export TEST_POSTJOB_DISABLE_RETRIES=True

# to avoid sending status report to dashboard and file metadata upload
export TEST_POSTJOB_NO_STATUS_UPDATE=True

# set some other args to emulate when calling the bootstrap
jobReturnCode=0
retryCount=0
maxRetries=10

# could run it, but all in all simply print the command and let user run it
echo "copy/paste and execute this command to re-run the post job"
echo "sh dag_bootstrap.sh POSTJOB ${jobClusterId} ${jobReturnCode} ${retryCount} ${maxRetries} $PJargs"

