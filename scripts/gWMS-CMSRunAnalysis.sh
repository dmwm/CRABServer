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
echo "User id:    $(id)"
echo "Local time: $(date)"
echo "Hostname:   $(hostname -f)"
echo "System:     $(uname -a)"
echo "Arguments are $@"

exec 2>&1
touch jobReport.json
touch WMArchiveReport.json

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

echo "======== CMSRunAnalsysis.sh at $(TZ=GMT date) FINISHING ========"

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
echo "======== WMAgent CMS environment load starting at $(TZ=GMT date) ========"
if [ -f "$VO_CMS_SW_DIR"/cmsset_default.sh ]
then  #   LCG style --
    echo "WN with a LCG style environment, thus using VO_CMS_SW_DIR=$VO_CMS_SW_DIR"
    . $VO_CMS_SW_DIR/cmsset_default.sh
elif [ -f "$OSG_APP"/cmssoft/cms/cmsset_default.sh ]
then  #   OSG style --
    echo "WN with an OSG style environment, thus using OSG_APP=$OSG_APP"
    . $OSG_APP/cmssoft/cms/cmsset_default.sh CMSSW_3_3_2
elif [ -f "$CVMFS"/cms.cern.ch/cmsset_default.sh ]
then
    echo "WN with CVMFS environment, thus using CVMFS=$CVMFS"
    . $CVMFS/cms.cern.ch/cmsset_default.sh
elif [ -f /cvmfs/cms.cern.ch/cmsset_default.sh ]
then  # ok, lets call it CVMFS then
    export CVMFS=/cvmfs/cms.cern.ch
    echo "WN missing VO_CMS_SW_DIR/OSG_APP/CVMFS environment variable, forcing it to CVMFS=$CVMFS"
    . $CVMFS/cmsset_default.sh
else
    echo "Error during job bootstrap: VO_CMS_SW_DIR, OSG_APP, CVMFS or /cvmfs were not found." >&2
    echo "  Because of this, we can't load CMSSW. Not good." >&2
    exit 11003
fi
echo "WMAgent bootstrap: WMAgent thinks it found the correct CMSSW setup script"
echo -e "======== WMAgent CMS environment load finished at $(TZ=GMT date) ========\n"

echo "======== python bootstrap for stageout at $(TZ=GMT date) STARTING ========"
# use python from COMP
# Python library required for Python2/Python3 compatibility through "future"
PY_FUTURE_VERSION=0.18.2
# First, decide which COMP ScramArch to use based on the required OS
if [ "$REQUIRED_OS" = "rhel7" ];
then
    WMA_SCRAM_ARCH=slc7_amd64_gcc630
else
    WMA_SCRAM_ARCH=slc6_amd64_gcc493
fi
echo "Job requires OS: $REQUIRED_OS, thus setting ScramArch to: $WMA_SCRAM_ARCH"

suffix=etc/profile.d/init.sh
if [ -d "$VO_CMS_SW_DIR"/COMP/"$WMA_SCRAM_ARCH"/external/python ]
then
    prefix="$VO_CMS_SW_DIR"/COMP/"$WMA_SCRAM_ARCH"/external/python
elif [ -d "$OSG_APP"/cmssoft/cms/COMP/"$WMA_SCRAM_ARCH"/external/python ]
then
    prefix="$OSG_APP"/cmssoft/cms/COMP/"$WMA_SCRAM_ARCH"/external/python
elif [ -d "$CVMFS"/COMP/"$WMA_SCRAM_ARCH"/external/python ]
then
    prefix="$CVMFS"/COMP/"$WMA_SCRAM_ARCH"/external/python
else
    echo "Error during job bootstrap: job environment does not contain the init.sh script." >&2
    echo "  Because of this, we can't load CMSSW. Not good." >&2
    exit 11004
fi

compPythonPath=`echo $prefix | sed 's|/python||'`
echo "WMAgent bootstrap: COMP Python path is: $compPythonPath"
latestPythonVersion=`ls -t "$prefix"/*/"$suffix" | head -n1 | sed 's|.*/external/python/||' | cut -d '/' -f1`
pythonMajorVersion=`echo $latestPythonVersion | cut -d '.' -f1`
pythonCommand="python"${pythonMajorVersion}
echo "WMAgent bootstrap: latest python release is: $latestPythonVersion"
source "$prefix/$latestPythonVersion/$suffix"
source "$compPythonPath/py2-future/$PY_FUTURE_VERSION/$suffix"

command -v $pythonCommand > /dev/null
rc=$?
if [[ $rc != 0 ]]
then
    echo "Error during job bootstrap: python isn't available on the worker node." >&2
    echo "  WMCore/WMAgent REQUIRES at least python2" >&2
    exit 11005
else
    echo "WMAgent bootstrap: found $pythonCommand at.."
    echo `which $pythonCommand`
fi
echo "======== python bootstrap for stageout at $(TZ=GMT date) FINISHED ========"

#echo "======== Attempting to notify HTCondor of file stageout ========"
# wrong syntax for chirping, also needs a proper classAd name. Keep commented line for a future fix
#condor_chirp phase output

echo "======== Stageout at $(TZ=GMT date) STARTING ========"
rm -f wmcore_initialized
# Note we prevent buffering of stdout/err -- this is due to observed issues in mixing of out/err for stageout plugins
PYTHONUNBUFFERED=1 python2.7 cmscp.py
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

