#!/bin/bash

#
# We used to rely on HTCondor file transfer plugins to handle output for us
# We changed this because HTCondor tosses the stdout/err, making the plugins
# difficult-to-impossible to run.
#

#
# We decided that it is cleaner if we do not change the environment in this script
# If a command needs a different environment, just call it in a subprocess with `time sh script.sh`
# as we do already for CMSRunAnalysis.sh and cmscp.sh
# It should not be needed, but in case something goes wrong and you need 
# 

echo "======== gWMS-CMSRunAnalysis.sh STARTING at $(TZ=GMT date) on $(hostname) ========"
echo "User id:    $(id)"
echo "Local time: $(date)"
echo "Hostname:   $(hostname -f)"
echo "System:     $(uname -a)"
echo "Arguments are $@"

# In the future, we could use condor_chirp to add a classad which records which
# is the current status of a job. 
# For example, we could set "bootstrap", "running", "stageout", "finished"
# We do not do it because we currently have no use for it.
#echo "======== Attempting to notify HTCondor of job bootstrap ========"
#condor_chirp set_job_attr_delayed Chirp_CRAB3_Job_Phase bootstrap

# redirect stderr to stdout, so that it all goes to job_out.*, leaving job_err.* empty
# see https://stackoverflow.com/a/13088401
exec 2>&1

#
# Saving START_TIME and when job finishes, check if runtime is not lower than 20m
# If it is lower, sleep the difference. Will not sleep if CRAB3_RUNTIME_DEBUG is set.
#
START_TIME=$(date +%s)

echo "======== Startup environment - STARTING ========"
# First of all, we save the startup environment.

# import some auxiliary functions from a script that is intented to be shared
# with WMCore
source ./submit_env.sh

# from ./submit_env.sh
save_env

echo "======== Startup environment - FINISHING ========"

echo "======== Auxiliary Functions - STARTING ========"
# auxiliary functions, needed when the job terminates
# we can consider making some of these fuctions less CRAB-oriented and moving them to ./submit_env.sh

print_file() {
    # similar in spirit to CMSRunAnalysis.py:logCMSSW()

    keepAtStart=1000
    keepAtEnd=3000
    maxLineLen=3000

    FILEPATH=$1
    cutLines=$(expr $(echo "[$FILEPATH]" | wc -L ) + $maxLineLen)
    maxLines=$(expr $keepAtStart + $keepAtEnd)
    numLines=$(wc -l $FILEPATH | cut -d ' ' -f 1)

    print_line() {
        # expects input to come from a pipe, such as
        # > echo $mylongline | print_line
        while read line; do echo "[$FILEPATH] $line" | awk -v cutLines=$cutLines '{print substr($0, 0, cutLines)}'; done
    }

    if [[ $numLines -gt $maxLines ]]; then
        head -n $keepAtStart $FILEPATH | print_line
        echo "[...]"
        tail -n $keepAtEnd $FILEPATH | print_line
    else
        cat $FILEPATH | print_line
    fi
    echo
}

check_file_exist() {
    FILEPATH=$1
    REGISTERENVVAR=$2
    if [[ -e $FILEPATH ]]; then
        echo "the file $FILEPATH exists"
        if [[ -s $FILEPATH ]]; then
            echo "the file $FILEPATH is non-empty. wc (newlines, words, bytes): " $(wc $FILEPATH)
            export $REGISTERENVVAR=true
            if [[ $3 == "printfile" ]]; then
                # used when the job terminates unexpectedly and we want to make sure that the
                # content of a file is written to stdout.
                print_file $FILEPATH
            fi
        fi
    else 
        echo "the file $FILEPATH does **NOT** exist"
    fi
}

term_signal_handler() {
    echo "The job received a termination signal: " $1

    # the job received a sigterm and it is likely that CMSRunAnalysis.py:logCMSSW()
    # did not run. We then print here the stdout+stderr of cmsRun.
    check_file_exist cmsRun-stdout.log.tmp DUMMY printfile

    # proceed with exit pipeline: this will call finish()
    exit
}

# Set a trap for SIGINT and SIGTERM signals
# it is possible that condor only sends sigterm, but i'd rather
# play it safe and trap also other termination signals
trap "term_signal_handler TERM" SIGTERM
trap "term_signal_handler INT"  SIGINT
trap "term_signal_handler HUP"  SIGHUP
trap "term_signal_handler QUIT" SIGQUIT

function finish {
  parse_cmsrun
  chirp_exit_code
  END_TIME=$(date +%s)
  DIFF_TIME=$((END_TIME-START_TIME))
  echo "Job started:  " $START_TIME ", " $(TZ=GMT date -d @$START_TIME)
  echo "Job finished: " $END_TIME ", " $(TZ=GMT date -d @$END_TIME)
  echo "Job Running time in seconds: " $DIFF_TIME
  if [ "X$CRAB3_RUNTIME_DEBUG" = "X" ];
  then
    if [ $DIFF_TIME -lt 60 ];
    then
      SLEEP_TIME=$((60 - DIFF_TIME))
      echo "Job runtime is less than 1 minute. Sleeping " $SLEEP_TIME
      sleep $SLEEP_TIME
    fi
  fi
}
# Trap all exits and execute finish function
trap finish EXIT

function chirp_exit_code {
    #Get the long exit code and set a classad using condor_chirp
    #The long exit code is taken from jobReport.json file if it exist
    #and is successfully loaded. It is taken from the EXIT_STATUS variable
    #instead (which contains the short exit coce)

    echo "======== Figuring out long exit code of the job for condor_chirp at $(TZ=GMT date)========"

    #check if the command condor_chirp exists
    command -v condor_chirp > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "Cannot find condor_chirp to set up the job exit code (ignoring, that's for monitoring purposes)"
        return
    fi

    #check if exitCode is in there and report it
    #otherwise check if EXIT_STATUS is set and report it
    if [[ $EXITTXT_NONEMPTY ]]; then
        #any error or exception will be printed to stderr and cause $? <> 0
        LONG_EXIT_CODE=$(cat jobReport.exitCode.txt)
        if [ $? -eq 0 ]; then
            echo "==== Long exit code of the job is $LONG_EXIT_CODE ===="
            condor_chirp set_job_attr_delayed Chirp_CRAB3_Job_ExitCode $LONG_EXIT_CODE
            CHIRP_EC=$?
        else
            unset EXITTXT_NONEMPTY
        fi
    fi
    if [[ ! $EXITTXT_NONEMPTY ]]; then
        # we want to execute this code block if:
        # - jobReport.exitCode.txt does not exist
        # - jobReport.exitCode.txt exists, but fails to be parsed
        echo "==== Failed to load the long exit code from jobReport.exitCode.txt. Falling back to short exit code ===="
        if [ -z "$EXIT_STATUS" ]; then
            echo "======== Short exit code also missing. Settint exit code to 80001 ========"
            condor_chirp set_job_attr_delayed Chirp_CRAB3_Job_ExitCode 80001
            CHIRP_EC=$?
        else
            echo "==== Short exit code of the job is $EXIT_STATUS ===="
            condor_chirp set_job_attr_delayed Chirp_CRAB3_Job_ExitCode $EXIT_STATUS
            CHIRP_EC=$?
        fi
    fi
    echo "======== Finished condor_chirp -ing the exit code of the job. Exit code of condor_chirp: $CHIRP_EC ========"

}

function parse_cmsrun {

    echo "======== Parsing output of cmsRun at $(TZ=GMT date)========"
    # We check which files that contain information about cmsRun execution exist.

    check_file_exist cmsRun-stdout.log.tmp DUMMY
    check_file_exist cmsRun-stdout.log DUMMY
    check_file_exist FrameworkJobReport.xml DUMMY
    check_file_exist jobReport.json.$CRAB_Id JRJSON_NONEMPTY
    check_file_exist jobReport.exitCode.txt EXITTXT_NONEMPTY

    echo "======== Finished - Parsing output of cmsRun at $(TZ=GMT date)========"

}

echo "======== Auxiliary Functions - FINISHING ========"

echo "======== gWMS-CMSRunAnalysis.sh STARTED at $(TZ=GMT date) on $(hostname) ========"

CRAB_oneEventMode=0
if [ "X$_CONDOR_JOB_AD" != "X" ];
then
    echo "======== HTCONDOR JOB SUMMARY at $(TZ=GMT date) START ========"
    ONE_EVENT_TEST=$(grep '^CRAB_oneEventMode =' $_CONDOR_JOB_AD | awk '{print $NF;}')
    if [[ $ONE_EVENT_TEST -eq 1 ]]; then
        CRAB_oneEventMode=1
    else
        CRAB_oneEventMode=0
    fi
    #The tail -1 is because the classad might be there on multiple lines and we want the last one
    CRAB_localOutputFiles=`grep '^CRAB_localOutputFiles =' $_CONDOR_JOB_AD | tail -1 | tr -d '"' | tr -d ',' | sed 's/CRAB_localOutputFiles = //'`
    export CRAB_Id=`grep '^CRAB_Id =' $_CONDOR_JOB_AD | tr -d '"' | tail -1 | awk '{print $NF;}'`
    export CRAB_Retry=`grep '^CRAB_Retry =' $_CONDOR_JOB_AD | tail -1 | tr -d '"' | awk '{print $NF;}'`
    JOB_CMSSite=`grep '^JOB_CMSSite =' $_CONDOR_JOB_AD | tr -d '"' | tail -1 | awk '{print $NF;}'`
    if [ "X$CRAB_Id" = "X" ];
    then
        echo "Unable to determine CRAB Id."
        exit 2
    fi
   echo "CRAB ID: $CRAB_Id"
   echo "Execution site: $JOB_CMSSite"
   echo "Current hostname: $(hostname)"
   echo "Output files: $CRAB_localOutputFiles"
   echo "==== HTCONDOR JOB AD CONTENTS START ===="
   while read i; do
       echo "== JOB AD: $i"
   done < $_CONDOR_JOB_AD
   echo "==== HTCONDOR JOB AD CONTENTS FINISH ===="
   echo "======== HTCONDOR JOB SUMMARY at $(TZ=GMT date) FINISH ========"
fi

touch jobReport.json
touch WMArchiveReport.json
#MM: Are these two lines needed?
touch jobReport.json.$CRAB_Id
touch WMArchiveReport.json.$CRAB_Id

echo "======== PROXY INFORMATION START at $(TZ=GMT date) ========"
voms-proxy-info -all
echo "======== PROXY INFORMATION FINISH at $(TZ=GMT date) ========"

echo "======== CMSRunAnalysis.sh at $(TZ=GMT date) STARTING ========"
time sh ./CMSRunAnalysis.sh "$@" --oneEventMode=$CRAB_oneEventMode
EXIT_STATUS=$?
echo "CMSRunAnalysis.sh complete at $(TZ=GMT date) with (short) exit status $EXIT_STATUS"

echo "======== CMSRunAnalysis.sh at $(TZ=GMT date) FINISHING ========"

mv jobReport.json jobReport.json.$CRAB_Id
mv WMArchiveReport.json WMArchiveReport.json.$CRAB_Id


if [[ $EXIT_STATUS == 137 ]]
then
  echo "Error: Job was killed. Check PostJob for kill reason."
  echo "Local time: $(date)"
  echo "Short exit status: $EXIT_STATUS"
  exit $EXIT_STATUS
fi

if [ $EXIT_STATUS -ne 0 ]
then
  echo "======== User Application failed (exit code =  $EXIT_STATUS) No stageout will be done"
  echo "======== gWMS-CMSRunAnalysis.sh FINISHING at $(TZ=GMT date) on $(hostname) with (short) status $EXIT_STATUS ========"
  echo "Local time: $(date)"
  #set -x
  echo "Short exit status: $EXIT_STATUS"
  exit $EXIT_STATUS
fi



echo "======== User application running completed. Prepare env. for stageout ==="
rm -f wmcore_initialized
time sh ./cmscp.sh
STAGEOUT_EXIT_STATUS=$?

if [ ! -e wmcore_initialized ];
then
    echo "======== ERROR: Unable to initialize WMCore at $(TZ=GMT date) ========"
    EXIT_STATUS=10043
fi

if [ $STAGEOUT_EXIT_STATUS -ne 0 ]; then
    #set -x
    if [ $EXIT_STATUS -eq 0 ]; then
        EXIT_STATUS=$STAGEOUT_EXIT_STATUS
    fi
fi
echo "======== Stageout at $(TZ=GMT date) FINISHING (short status $STAGEOUT_EXIT_STATUS) ========"

echo "======== gWMS-CMSRunAnalysis.sh FINISHING at $(TZ=GMT date) on $(hostname) with (short) status $EXIT_STATUS ========"
echo "Local time: $(date)"
#set -x
echo "Short exit status: $EXIT_STATUS"
exit $EXIT_STATUS

