#!/bin/bash

#
# We used to rely on HTCondor file transfer plugins to handle output for us
# We changed this because HTCondor tosses the stdout/err, making the plugins
# difficult-to-impossible to run.
#
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
    CRAB_Dest=`grep '^CRAB_Dest =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
    if [ "X$CRAB_Dest" = "X" ];
    then
        print "Unable to determine CRAB output destination directory"
        sleep 20m
        exit 2
    fi
    ONE_EVENT_TEST=$(grep '^CRAB_oneEventMode =' $_CONDOR_JOB_AD | awk '{print $NF;}')
    if [[ $ONE_EVENT_TEST -eq 1 ]]; then
        CRAB_oneEventMode=1
    else
        CRAB_oneEventMode=0
    fi
    CRAB_localOutputFiles=`grep '^CRAB_localOutputFiles =' $_CONDOR_JOB_AD | tr -d '"' | tr -d ',' | sed 's/CRAB_localOutputFiles = //'`
    CRAB_Id=`grep '^CRAB_Id =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
    JOB_CMSSite=`grep '^JOB_CMSSite =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
    if [ "X$CRAB_Id" = "X" ];
    then
        print "Unable to determine CRAB Id."
        sleep 20m
        exit 2
    fi
   echo "CRAB ID: $CRAB_Id"
   echo "Execution site: $JOB_CMSSite"
   echo "Current hostname: $(hostname)"
   echo "Destination site: $CRAB_Dest"
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
# Protect against missing *.sh initialization scripts.
if [[ $EXIT_STATUS == 1 ]]
then
  sleep 20m
fi

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
if [ "x" != "x$VO_CMS_SW_DIR" ]
then
    set +x
	. $VO_CMS_SW_DIR/cmsset_default.sh
    set -x
#   OSG style --
elif [ "x" != "x$OSG_APP" ]
then
    set +x
	. $OSG_APP/cmssoft/cms/cmsset_default.sh CMSSW_3_3_2
    set -x
elif [ -e /cvmfs/cms.cern.ch ]
then
	export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
    set +x
	. $VO_CMS_SW_DIR/cmsset_default.sh
    set -x
else
	echo "Error: OSG_APP nor VO_CMS_SW_DIR environment variables were set" >&2
	echo "Error: CVMFS is not present" >&2
	echo "Error: Because of this, we can't bootstrap to attempt stageout." >&2
        sleep 20m
	exit 2
fi

if [ -e $VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/python/2.6.4/etc/profile.d/init.sh ]
then
    set +x
	. $VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/python/2.6.4/etc/profile.d/init.sh
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/openssl/0.9.7m/lib:$VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/bz2lib/1.0.5/lib
    set -x
elif [ -e $OSG_APP/cmssoft/cms/COMP/slc5_amd64_gcc434/external/python/2.6.4/etc/profile.d/init.sh ]
then
    set +x
	. $OSG_APP/cmssoft/cms/COMP/slc5_amd64_gcc434/external/python/2.6.4/etc/profile.d/init.sh
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$OSG_APP/cmssoft/cms/COMP/slc5_amd64_gcc434/external/openssl/0.9.7m/lib:$OSG_APP/cmssoft/cms/COMP/slc5_amd64_gcc434/external/bz2lib/1.0.5/lib
    set -x
fi
command -v python2.6 > /dev/null
rc=$?
set +x
if [[ $rc != 0 ]]
then
	echo "Error: Python2.6 isn't available on this worker node." >&2
	echo "Error: execution of job stageout wrapper REQUIRES python2.6" >&2
	sleep 20m
        exec sh ./DashboardFailure.sh 10043
else
	echo "Found python2.6 at:"
	echo `which python2.6`
fi
echo "======== python2.6 bootstrap for stageout at $(TZ=GMT date) FINISHING ========"

echo "======== Attempting to notify HTCondor of file stageout ========"
condor_chirp phase output

echo "======== Stageout at $(TZ=GMT date) STARTING ========"
rm -f wmcore_initialized
# Note we prevent buffering of stdout/err -- this is due to observed issues in mixing of out/err for stageout plugins
PYTHONUNBUFFERED=1 ./cmscp.py
STAGEOUT_EXIT_STATUS=$?

if [ ! -e wmcore_initialized ];
then
    echo "======== ERROR: Unable to initialize WMCore at $(TZ=GMT date) ========"
    sleep 20m
    exec sh ./DashboardFailure.sh 10043
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

