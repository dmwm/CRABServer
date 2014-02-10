#!/bin/sh

#
# We used to rely on HTCondor file transfer plugins to handle output for us
# We changed this because HTCondor tosses the stdout/err, making the plugins
# difficult-to-impossible to run.
#
echo "======== gWMS-CMSRunAnalysis.py STARTING at $(date) on $(hostname) ========"
echo "Arguments are $@"

exec 2>&1
touch jobReport.json

echo "SCRAM_ARCH=$SCRAM_ARCH"
CRAB_oneEventMode=0
if [ "X$_CONDOR_JOB_AD" != "X" ];
then
    echo "======== HTCONDOR JOB SUMMARY START ========"
    date
    CRAB_Dest=`grep '^CRAB_Dest =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
    if [ "X$CRAB_Dest" = "X" ];
    then
        print "Unable to determine CRAB output destination directory"
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
   echo "======== HTCONDOR JOB SUMMARY at $(date) FINISH ========"
fi

echo "======== CMSRunAnalysis.sh at $(date) STARTING ========"
time sh ./CMSRunAnalysis.sh "$@" --oneEventMode=$CRAB_oneEventMode
EXIT_STATUS=$?
echo "CMSRunAnalysis.sh complete at $(date) with exit status $EXIT_STATUS"
echo "======== CMSRunAnalsysis.sh at $(date) FINISHING ========"

mv jobReport.json jobReport.json.$CRAB_Id

echo "======== python2.6 bootstrap for stageout at $(date) STARTING ========"
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
	echo "Error: job execution REQUIRES python2.6" >&2
	exit
else
	echo "I found python2.6 at.."
	echo `which python2.6`
fi
echo "======== python2.6 bootstrap for stageout at $(date) FINISHING ========"

echo "======== Stageout at $(date) STARTING ========"
# Note we prevent buffering of stdout/err -- this is due to observed issues in mixing of out/err for stageout plugins
PYTHONUNBUFFERED=1 ./cmscp.py
STAGEOUT_EXIT_STATUS=$?
if [ $STAGEOUT_EXIT_STATUS -ne 0 ]; then
    set -x
    if [ $EXIT_STATUS -eq 0 ]; then
        EXIT_STATUS=$STAGEOUT_EXIT_STATUS
    fi
fi
echo "======== Stageout at $(date) FINISHING (status $STAGEOUT_EXIT_STATUS) ========"

echo "======== gWMS-CMSRunAnalysis.py FINISHING at $(date) on $(hostname) with status $EXIT_STATUS ========"
set -x
exit $EXIT_STATUS

