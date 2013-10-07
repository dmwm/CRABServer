#!/bin/sh

#
# We used to rely on HTCondor file transfer plugins to handle output for us
# We changed this because HTCondor tosses the stdout/err, making the plugins
# difficult-to-impossible to run.
#
echo "Beginning gWMS-CMSRunAnalysis.py at $(date)"
echo "Arguments are $@"
set -x

# target args are '-a $(CRAB_Archive)' '--sourceURL=$(CRAB_ISB)' '--jobNumber=$(CRAB_Id)' '--cmsswVersion=$(CRAB_JobSW)' --scramArch=$(CRAB_JobArch) '--inputFile=$(inputFiles)' '--runAndLumis=$(runAndLumiMask)' -o $(CRAB_AdditionalOutputFiles)

function extractArg() {
    # pulls args first from classad then from environment
    # if neither exists, bomb
    local AD_VAL=''
    if [ "X$_CONDOR_JOB_AD" != "X" ]; then
        local AD_VAL=`grep "$1 =" $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
    fi
    eval local ENV_VAL=\$$1
    if [ "X$AD_VAL" != "X" ]; then
        echo $AD_VAL
        return
    elif [ "X$ENV_VAL" != "X" ]; then
        echo $ENV_VAL
        return
    else
        if [ "X$2" == "X" ]; then
            echo "Couldn't get a value for $1" 1>&2
            exit 1
        else
            echo $2
            return
        fi
    fi

}

touch jobReport.json
if [ "x" == "x$X509_USER_PROXY" ] || [ ! -e $X509_USER_PROXY ]]; then
    echo "ERROR: Couldn't find a valid proxy at $X509_USER_PROXY"
    echo "Got the following environment variables that may help:"
    env | grep -i proxy
    env | grep 509
    echo "Got the following things in the job ad that may help:"
    cat $_CONDOR_JOB_AD | sort
    exit 5
fi


/scratch/meloam/auto_edntuple/config_QCD_Pt_20_MuEnrichedPt_15_TuneZ2star_8TeV_pythia6_v3.cfg
echo "Got the following from the classad"
cat $_CONDOR_JOB_AD

hostname
echo "Starting CMSRunAnalysis.sh at $(date)"
# target args are '-a $(CRAB_Archive)' '--sourceURL=$(CRAB_ISB)' '--jobNumber=$(CRAB_Id)' '--cmsswVersion=$(CRAB_JobSW)' --scramArch=$(CRAB_JobArch) '--inputFile=$(inputFiles)' '--runAndLumis=$(runAndLumiMask)' -o $(CRAB_AdditionalOutputFiles)
CRAB_Archive=$(extractArg CRAB_Archive) || exit 1
CRAB_ISB=$(extractArg CRAB_ISB) || exit 1 
CRAB_Id=$(extractArg CRAB_Id) || exit 1 
CRAB_JobSW=$(extractArg CRAB_JobSW) || exit 1 
CRAB_JobArch=$(extractArg CRAB_JobArch) || exit 1 
CRAB_AdditionalOutputFiles=$(extractArg CRAB_AdditionalOutputFiles) || exit 1
# put this back into the commandline args until quoting can get figured
# out
CRAB_InputFiles=$(extractArg CRAB_InputFiles) || exit 1
CRAB_RunAndLumiMask=$(extractArg CRAB_RunAndLumiMask) || exit 1

# Need a unify how we have debug stuff pierce through all the layers
CRAB_oneEventMode=$(extractArg CRAB_oneEventMode 0)
if [ "x" != "x$CRAB_TASKMANAGER_TARBALL" ]; then
    export CRAB_TASKMANAGER_TARBALL
fi

touch jobReport.json.$CRAB_Id
if [ "x" == "x$X509_USER_PROXY" ] || [ ! -e $X509_USER_PROXY ]]; then
    echo "ERROR: Couldn't find a valid proxy at $X509_USER_PROXY"
    echo "Got the following environment variables that may help:"
    env | grep -i proxy
    env | grep 509
    echo "Got the following things in the job ad that may help:"
    cat $_CONDOR_JOB_AD | sort
    exit 5
fi

time sh ./CMSRunAnalysis.sh  -a ${CRAB_Archive} "--sourceURL=${CRAB_ISB}" "--jobNumber=${CRAB_Id}" "--cmsswVersion=${CRAB_JobSW}" --scramArch=${CRAB_JobArch}  "-o ${CRAB_AdditionalOutputFiles}" --oneEventMode=$CRAB_oneEventMode "$@"
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
    set +X
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

