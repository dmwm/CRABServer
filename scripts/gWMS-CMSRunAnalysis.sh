#!/bin/bash

#
# We used to rely on HTCondor file transfer plugins to handle output for us
# We changed this because HTCondor tosses the stdout/err, making the plugins
# difficult-to-impossible to run.
#

# On some sites we know there was some problems with environment cleaning
# with using 'env -i'. To overcome this issue, whenever we start a job, we have
# to save full current environment into file, and whenever it is needed we can load
# it. Be aware, that there are some read-only variables, like: BASHOPTS, BASH_VERSINFO,
# EUID, PPID, SHELLOPTS, UID, etc.
set > startup_environment.sh
sed -e 's/^/export /' startup_environment.sh > tmp_env.sh
mv tmp_env.sh startup_environment.sh
export JOBSTARTDIR=$PWD

# Saving START_TIME and when job finishes, check if runtime is not lower than 20m
# If it is lower, sleep the difference. Will not sleep if CRAB3_RUNTIME_DEBUG is set.
START_TIME=$(date +%s)
function finish {
  chirp_exit_code
  END_TIME=$(date +%s)
  DIFF_TIME=$((END_TIME-START_TIME))
  echo "Job Running time in seconds: " $DIFF_TIME
  if [ "X$CRAB3_RUNTIME_DEBUG" = "X" ];
  then
    if [ $DIFF_TIME -lt 1200 ];
    then
      SLEEP_TIME=$((1200 - DIFF_TIME))
      SLEEP_TIME=1
      echo "Job runtime is less than 20minutes. Sleeping " $SLEEP_TIME
      sleep $SLEEP_TIME
    fi
  fi
}
# Trap all exits and execute finish function
trap finish EXIT

function DashboardFailure {
    if [ -f ./DashboardFailure.sh ]; then
        exec sh ./DashboardFailure.sh $1
    else
        exit $1
    fi
}

function chirp_exit_code {
    #Get the long exit code and set a classad using condor_chirp
    #The long exit code is taken from jobReport.json file if it exist
    #and is successfully loaded. It is taken from the EXIT_STATUS variable
    #instead (which contains the short exit coce)

    #check if the command condor_chirp exists
    command -v condor_chirp > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "Cannot find condor_chirp to set up the job exit code (ignoring, that's for monitoring purposes)"
        return
    fi
    #check if exitCode is in there and report it
    echo "======== Figuring out long exit code of the job for condor_chirp ========"
    #any error or exception will be printed to stderr and cause $? <> 0
    LONG_EXIT_CODE=$(python << END
from __future__ import print_function
import json
with open("jobReport.json.$CRAB_Id") as fd:
    data = json.load(fd)
print(data['exitCode'])
END
)
    if [ $? -eq 0 ]; then
        echo "==== Long exit code of the job is $LONG_EXIT_CODE ===="
        condor_chirp set_job_attr_delayed Chirp_CRAB3_Job_ExitCode $LONG_EXIT_CODE
        CHIRP_EC=$?
    else
        echo "==== Failed to load the long exit code from jobReport.json.$CRAB_Id. Falling back to short exit code ===="
        #otherwise checjk if EXIT_STATUS is set and report it
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

echo "======== gWMS-CMSRunAnalysis.sh STARTING at $(TZ=GMT date) on $(hostname) ========"
echo "Local time : $(date)"
echo "Current system : $(uname -a)"
echo "Arguments are $@"

exec 2>&1
touch jobReport.json

echo "SCRAM_ARCH=$SCRAM_ARCH"
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
    CRAB_localOutputFiles=`grep '^CRAB_localOutputFiles =' $_CONDOR_JOB_AD | tr -d '"' | tr -d ',' | sed 's/CRAB_localOutputFiles = //'`
    export CRAB_Id=`grep '^CRAB_Id =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
    export CRAB_Retry=`grep '^CRAB_Retry =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
    JOB_CMSSite=`grep '^JOB_CMSSite =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
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

touch jobReport.json.$CRAB_Id

echo "======== PROXY INFORMATION START at $(TZ=GMT date) ========"
voms-proxy-info -all
echo "======== PROXY INFORMATION FINISH at $(TZ=GMT date) ========"

echo "======== CMSRunAnalysis.sh at $(TZ=GMT date) STARTING ========"
time sh ./CMSRunAnalysis.sh "$@" --oneEventMode=$CRAB_oneEventMode
EXIT_STATUS=$?
echo "CMSRunAnalysis.sh complete at $(TZ=GMT date) with (short) exit status $EXIT_STATUS"

echo "======== CMSRunAnalsysis.sh at $(TZ=GMT date) FINISHING ========"

mv jobReport.json jobReport.json.$CRAB_Id

if [[ $EXIT_STATUS == 137 ]]
then
  echo "Error: Job was killed. Check PostJob for kill reason."
  echo "Local time: $(date)"
  echo "Short exit status: $EXIT_STATUS"
  exit $EXIT_STATUS
fi

echo "======== python2.6 bootstrap for stageout at $(TZ=GMT date) STARTING ========"
set -x
### Need python2.6 for stageout also
## CMS_PATH env is needed for WMCore stageout plugins.
if [ -f "$VO_CMS_SW_DIR"/cmsset_default.sh ]
then
    set +x
    export CMS_PATH=$VO_CMS_SW_DIR
    . $VO_CMS_SW_DIR/cmsset_default.sh
    set -x
elif [ -f "$OSG_APP"/cmssoft/cms/cmsset_default.sh ]
then
    set +x
    export CMS_PATH=$OSG_APP/cmssoft/cms/
    . $OSG_APP/cmssoft/cms/cmsset_default.sh CMSSW_3_3_2
    set -x
elif [ -f /cvmfs/cms.cern.ch/cmsset_default.sh ]
then
    export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
    export CMS_PATH=$VO_CMS_SW_DIR
    set +x
    . $VO_CMS_SW_DIR/cmsset_default.sh
    set -x
else
	echo "Error: OSG_APP nor VO_CMS_SW_DIR environment variables were set" >&2
	echo "Error: CVMFS is not present" >&2
	echo "Error: Because of this, we can't bootstrap to attempt stageout." >&2
	exit 2
fi

if [ -e $VO_CMS_SW_DIR/COMP/slc6_amd64_gcc493/external/python/2.7.6/etc/profile.d/init.sh ]
then
    set +x
	. $VO_CMS_SW_DIR/COMP/slc6_amd64_gcc493/external/python/2.7.6/etc/profile.d/init.sh
	#export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$VO_CMS_SW_DIR/COMP/slc6_amd64_gcc481/external/openssl/1.0.1p/lib:$VO_CMS_SW_DIR/COMP/slc6_amd64_gcc481/external/bz2lib/1.0.6/lib
    set -x
elif [ -e $OSG_APP/cmssoft/cms/COMP/slc6_amd64_gcc493/external/python/2.7.6/etc/profile.d/init.sh ]
then
    set +x
	. $OSG_APP/cmssoft/cms/COMP/slc6_amd64_gcc493/external/python/2.7.6/etc/profile.d/init.sh
    #export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$OSG_APP/cmssoft/cms/COMP/slc6_amd64_gcc481/external/openssl/1.0.1p/lib:$OSG_APP/cmssoft/cms/COMP/slc6_amd64_gcc481/external/bz2lib/1.0.6/lib
    set -x
fi
command -v python2.7 > /dev/null
rc=$?
set +x
if [[ $rc != 0 ]]
then
    echo "Error: Python2.7 isn't available on this worker node." >&2
    echo "Error: execution of job stageout wrapper REQUIRES python2.6" >&2
    EXIT_STATUS=10043
    DashboardFailure $EXIT_STATUS
else
    echo "Found python2.7 at:"
    echo `which python2.7`
fi
echo "======== python2.7 bootstrap for stageout at $(TZ=GMT date) FINISHING ========"

echo "======== Attempting to notify HTCondor of file stageout ========"
condor_chirp phase output

echo "======== Stageout at $(TZ=GMT date) STARTING ========"
rm -f wmcore_initialized
# Note we prevent buffering of stdout/err -- this is due to observed issues in mixing of out/err for stageout plugins
PYTHONUNBUFFERED=1 python2.7 cmscp.py
STAGEOUT_EXIT_STATUS=$?

if [ ! -e wmcore_initialized ];
then
    echo "======== ERROR: Unable to initialize WMCore at $(TZ=GMT date) ========"
    EXIT_STATUS=10043
    DashboardFailure $EXIT_STATUS
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

