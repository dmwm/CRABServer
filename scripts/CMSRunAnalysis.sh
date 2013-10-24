#!/bin/bash
set -x
#touch Report.pkl

# should be a bit nicer than before
echo "Starting CMSRunAnalysis.sh at $(date)..."

### source the CMSSW stuff using either OSG or LCG style entry env. variables
###    (incantations per oli's instructions)
#   LCG style --
if [ "x" != "x$VO_CMS_SW_DIR" ]
then
    echo 'LCG style'
    set +x
    .  $VO_CMS_SW_DIR/cmsset_default.sh
    set -x
    declare -a VERSIONS
    VERSIONS=($(ls $VO_CMS_SW_DIR/$SCRAM_ARCH/external/python | grep 2.6))
    PY_PATH=$VO_CMS_SW_DIR/$SCRAM_ARCH/external/python
    echo 'python version: ' $VERSIONS
#   OSG style --
elif [ "x" != "x$OSG_APP" ]
then
    echo 'OSG style'  
    set +x
	. $OSG_APP/cmssoft/cms/cmsset_default.sh
    set -x
    declare -a VERSIONS
    VERSIONS=($(ls $OSG_APP/cmssoft/cms/$SCRAM_ARCH/external/python | grep 2.6))
    PY_PATH=$OSG_APP/cmssoft/cms/$SCRAM_ARCH/external/python 
    echo 'python version: ' $VERSIONS
elif [ -e /cvmfs/cms.cern.ch ]
then
    export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
    set +x
    . $VO_CMS_SW_DIR/cmsset_default.sh
    . /cvmfs/cms.cern.ch/slc5_amd64_gcc461/external/python/2.6.4-cms2/etc/profile.d/init.sh
    set -x
    #. /cvmfs/cms.cern.ch/COMP/slc5_amd64_gcc461/external/python/2.6.4-cms2/etc/profile.d/init.sh
    #. /cvmfs/cms.cern.ch/COMP/slc5_amd64_gcc434/external/openssl/0.9.8e-comp2/etc/profile.d/init.sh
else
	echo "Error: neither OSG_APP, CVMFS, nor VO_CMS_SW_DIR environment variables were set" >&2
	echo "Error: Because of this, we can't load CMSSW. Not good." >&2
	exit 2
fi
echo "I think I found the correct CMSSW setup script"

# check for Python2.6 installation 
N_PYTHON26=${#VERSIONS[*]}
if [ $N_PYTHON26 -lt 1 ]; then
    # The check later with command is sufficient
    :
else
    VERSION=${VERSIONS[0]}
    echo "Python 2.6 found in $PY_PATH/$VERSION";
    PYTHON26=$PY_PATH/$VERSION
    # Initialize CMS Python 2.6
    set +x
    source $PY_PATH/$VERSION/etc/profile.d/init.sh
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

echo "Current environment is"
env

if [[ "X$CRAB_TASKMANAGER_TARBALL" == 'X' ]]; then
    # Didn't ask to bring a tarball with us
    wget http://common-analysis-framework.cern.ch/CMSRunAnaly.tgz
    tar xvfzm CMSRunAnaly.tgz
elif [[ $CRAB_TASKMANAGER_TARBALL == 'local' ]]; then
    # Tarball was shipped with condor
    tar xvzmf TaskManagerRun.tar.gz
else
    # Allow user to override the choice
    curl $CRAB_TASKMANAGER_TARBALL | tar xvzm
fi
export PYTHONPATH=`pwd`/CRAB3.zip:`pwd`/WMCore.zip:$PYTHONPATH
ls
echo "Now running the CMSRunAnalysis.py job in `pwd`..."
# Fixing a poorly-chosen abbreviation
if [[ -e CMSRunAnalysis.py ]]; then
    python2.6 CMSRunAnalysis.py -r "`pwd`" "$@"
    jobrc=$?
else
    python2.6 CMSRunAnaly.py -r "`pwd`" "$@"
    jobrc=$?
fi
echo "The job had an exit code of $jobrc "

cat jobLog.*
cat scramOutput.log
exit $jobrc

_
