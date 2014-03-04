#!/bin/bash
exec 2>&1
#touch Report.pkl

# should be a bit nicer than before
echo "======== CMSRunAnalysis.sh STARTING at $(date) ========"

### source the CMSSW stuff using either OSG or LCG style entry env. variables
###    (incantations per oli's instructions)
#   LCG style --
echo "==== CMSSW pre-execution environment bootstrap STARTING ===="
set -x
if [ "x" != "x$VO_CMS_SW_DIR" ]
then
    echo 'LCG style'
    set +x
    .  $VO_CMS_SW_DIR/cmsset_default.sh
    rc=$?
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
    rc=$?
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
    rc=$?
    . /cvmfs/cms.cern.ch/slc5_amd64_gcc461/external/python/2.6.4-cms2/etc/profile.d/init.sh
    set -x
else
	echo "Error: neither OSG_APP, CVMFS, nor VO_CMS_SW_DIR environment variables were set" >&2
	echo "Error: Because of this, we can't load CMSSW. Not good." >&2
	sleep 20m
	exit ./DashboardFailure.sh 10031
fi
set +x
if [[ $rc != 0 ]]
then
    echo "==== Sourcing cmsset_default.sh failed at $(date).  Sleeping for 20 minutes. ===="
    sleep 20m
    exit ./DashboardFailure.sh 10032
else
    echo "==== CMSSW pre-execution environment bootstrap FINISHING at $(date) ===="
fi

echo "==== Python2.6 discovery STARTING ===="
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
fi

command -v python2.6 > /dev/null
rc=$?
if [[ $rc != 0 ]]
then
	echo "Error: Python2.6 isn't available on this worker node." >&2
	echo "Error: job execution REQUIRES python2.6" >&2
	sleep 20m
	exit $rc
else
	echo "I found python2.6 at.."
	echo `which python2.6`
fi
python2.6 -c ''
rc=$?
if [[ $rc != 0 ]]
then
	echo "Error: python2.6 is not functional."
	sleep 20m
	./DashboardFailure.sh 10043
	exit $?
fi

echo "==== Python2.6 discovery FINISHING at $(date) ===="

echo "======== Current environment dump STARTING ========"
for i in `env`; do
  echo "== ENV: $i"
done
echo "======== Current environment dump STARTING ========"

echo "======== Tarball initialization STARTING at $(date) ========"
set -x
if [[ "X$CRAB_RUNTIME_TARBALL" == "X" ]]; then
    # Didn't ask to bring a tarball with us
    wget http://common-analysis-framework.cern.ch/CMSRunAnaly.tgz || exit 10042
    tar xvfzm CMSRunAnaly.tgz || exit 10042
elif [[ $CRAB_RUNTIME_TARBALL == "local" ]]; then
    # Tarball was shipped with condor
    tar xvzmf CMSRunAnalysis.tar.gz || exit 10042
else
    # Allow user to override the choice
    curl $CRAB_RUNTIME_TARBALL | tar xvzm || exit 10042
fi
export PYTHONPATH=`pwd`/CRAB3.zip:`pwd`/WMCore.zip:$PYTHONPATH
set +x
echo "======== Tarball initialization FINISHING at $(date) ========"
echo "==== Local directory contents dump STARTING ===="
echo "PWD: `pwd`"
for i in `ls`; do
  echo "== DIR: $i"
done
echo "==== Local directory contents dump FINISHING ===="
echo "======== CMSRunAnalysis.py STARTING at $(date) ========"
echo "Now running the CMSRunAnalysis.py job in `pwd`..."
set -x
python2.6 CMSRunAnalysis.py -r "`pwd`" "$@"
jobrc=$?
set +x
echo "== The job had an exit code of $jobrc "
echo "======== CMSRunAnalysis.py FINISHING at $(date) ========"

if [ -e scramOutput.log ]; then
  echo "==== SCRAM interaction log contents dump STARTING ===="
  cat scramOutput.log
  echo "==== SCRAM interaction log contents dump FINISHING ===="
else
  echo "ERROR: scramOutput.log does not exist."
fi
exit $jobrc

_
