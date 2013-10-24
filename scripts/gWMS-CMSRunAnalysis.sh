#!/bin/sh

#
# We used to rely on HTCondor file transfer plugins to handle output for us
# We changed this because HTCondor tosses the stdout/err, making the plugins
# difficult-to-impossible to run.
#
echo "Beginning gWMS-CMSRunAnalysis.py at $(date)"
echo "Arguments are $@"

set -x
touch jobReport.json

echo "SCRAM_ARCH=$SCRAM_ARCH"
CRAB_oneEventMode=0
if [ "X$_CONDOR_JOB_AD" != "X" ];
then
    echo "The condor job ad is"
    cat $_CONDOR_JOB_AD
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
    CRAB_localOutputFiles=`grep '^CRAB_localOutputFiles =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
    CRAB_Id=`grep '^CRAB_Id =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
    if [ "X$CRAB_Id" = "X" ];
    then
        print "Unable to determine CRAB Id."
        exit 2
    fi
   echo "Output files: $CRAB_localOutputFiles"
   echo "CRAB ID: $CRAB_Id"
   echo "Destination: $CRAB_Dest"
fi

hostname
echo "Starting CMSRunAnalysis.sh at $(date)"
time sh ./CMSRunAnalysis.sh "$@" --oneEventMode=$CRAB_oneEventMode
EXIT_STATUS=$?
echo "CMSRunAnalysis.sh Complete at $(date)"

mv jobReport.json jobReport.json.$CRAB_Id

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
	echo "Error: neither OSG_APP nor VO_CMS_SW_DIR environment variables were set" >&2
	echo "Error: Because of this, we can't load CMSSW. Not good." >&2
	exit 2
fi
echo "I think I found the correct CMSSW setup script"

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
if [[ $rc != 0 ]]
then
	echo "Error: Python2.6 isn't available on this worker node." >&2
	echo "Error: job execution REQUIRES python2.6" >&2
	exit
else
	echo "I found python2.6 at.."
	echo `which python2.6`
fi

echo "Starting Stageout"
./cmscp.py "$PWD/cmsRun-stderr.log?compressCount=3&remoteName=cmsRun_$CRAB_Id.log" "$CRAB_Dest/cmsRun-stderr.log?compressCount=3&remoteName=cmsRun_$CRAB_Id.log"  || exit $?
./cmscp.py "$PWD/cmsRun-stdout.log?compressCount=3&remoteName=cmsRun_$CRAB_Id.log" "$CRAB_Dest/cmsRun-stdout.log?compressCount=3&remoteName=cmsRun_$CRAB_Id.log" || exit $?
./cmscp.py "$PWD/FrameworkJobReport.xml?compressCount=3&remoteName=cmsRun_$CRAB_Id.log" "$CRAB_Dest/FrameworkJobReport.xml?compressCount=3&remoteName=cmsRun_$CRAB_Id.log" || exit $?
OIFS=$IFS
IFS=" ,"
for file in $CRAB_localOutputFiles; do
    ./cmscp.py "$PWD/$file" $CRAB_Dest/$file
done
echo "Finished stageout"

exit $EXIT_STATUS

