#!/bin/bash
exec 2>&1

sigterm() {
  echo "ERROR: Job was killed. Logging ulimits:"
  ulimit -a
  echo "Logging free memory info:"
  free -m
  echo "Logging disk usage:"
  df -h
  echo "Logging disk usage in directory:"
  du -h
  exec sh ./DashboardFailure.sh 50669
  if [ ! -e logCMSSWSaved.txt ];
  then
    python -c "import CMSRunAnalysis; logCMSSW()"
  fi
}

# should be a bit nicer than before
echo "======== CMSRunAnalysis.sh STARTING at $(TZ=GMT date) ========"
echo "Local time : $(date)"
echo "Current system : $(uname -a)"
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
    VERSIONS=($(ls $VO_CMS_SW_DIR/$SCRAM_ARCH/external/python | egrep '2.[67]'))
    PY_PATH=$VO_CMS_SW_DIR/$SCRAM_ARCH/external/python
    echo 'python version: ' $VERSIONS
#   OSG style --
elif [ "x" != "x$OSG_APP" ]
then
    echo 'OSG style'  
    set +x
    . $OSG_APP/cmssoft/cms/cmsset_default.sh
    rc=$?
    if [[ $rc != 0 ]]
    then
        echo "==== Sourcing cmsset_default.sh failed at $(TZ=GMT date).  Sleeping for 20 minutes. ===="
        sleep 20m
        tar xvzmf CMSRunAnalysis.tar.gz || exit 10042
        exec sh ./DashboardFailure.sh 10032
    fi
    set -x
    declare -a VERSIONS
    VERSIONS=($(ls $OSG_APP/cmssoft/cms/$SCRAM_ARCH/external/python | egrep '2.[67]'))
    PY_PATH=$OSG_APP/cmssoft/cms/$SCRAM_ARCH/external/python 
    echo 'python version: ' $VERSIONS
elif [ -e /cvmfs/cms.cern.ch ]
then
    export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
    set +x
    . $VO_CMS_SW_DIR/cmsset_default.sh
    rc=$?
    set -x
    declare -a VERSIONS
    VERSIONS=($(ls /cvmfs/cms.cern.ch/$SCRAM_ARCH/external/python | egrep '2.[67]'))
    PY_PATH=/cvmfs/cms.cern.ch/$SCRAM_ARCH/external/python
    echo 'python version: ' $VERSIONS
else
	echo "Error: neither OSG_APP, CVMFS, nor VO_CMS_SW_DIR environment variables were set" >&2
	echo "Error: Because of this, we can't load CMSSW. Not good." >&2
	sleep 20m
        tar xvzmf CMSRunAnalysis.tar.gz || exit 10042
	exec sh ./DashboardFailure.sh 10031
fi
set +x
if [[ $rc != 0 ]]
then
    echo "==== Sourcing cmsset_default.sh failed at $(TZ=GMT date).  Sleeping for 20 minutes. ===="
    sleep 20m
    tar xvzmf CMSRunAnalysis.tar.gz || exit 10042
    exec sh ./DashboardFailure.sh 10032
else
    echo "==== CMSSW pre-execution environment bootstrap FINISHING at $(TZ=GMT date) ===="
fi

echo "==== Python discovery STARTING ===="
# check for Python installation 
N_PYTHON26=${#VERSIONS[*]}
if [ $N_PYTHON26 -lt 1 ]; then
    echo "Error: Unable to find a CMS version of python."
    echo "CRAB3 requires the CMS version of python to function."
    sleep 20m
    exec sh ./DashboardFailure.sh 10043
else
    VERSION=${VERSIONS[0]}
    echo "Python found in $PY_PATH/$VERSION";
    PYTHON26=$PY_PATH/$VERSION
    # Initialize CMS Python
    set +x
    source $PY_PATH/$VERSION/etc/profile.d/init.sh
    if [[ $? != 0 ]]
    then
        echo "Error: Unable to source python environment setup."
        sleep 20m
        exec sh ./DashboardFailure.sh 10043
    fi
fi

command -v python > /dev/null
rc=$?
if [[ $rc != 0 ]]
then
	echo "Error: Python wasn't found on this worker node after source CMS version." >&2
	echo "Error: job execution REQUIRES a working python" >&2
	sleep 20m
	exec sh ./DashboardFailure.sh 10043
else
	echo "I found python at.."
	echo `which python`
fi
python -c ''
rc=$?
if [[ $rc != 0 ]]
then
	echo "Error: python is not functional."
	sleep 20m
	exec sh ./DashboardFailure.sh 10043
fi

echo "==== Python discovery FINISHING at $(TZ=GMT date) ===="

echo "======== Current environment dump STARTING ========"
for i in `env`; do
  echo "== ENV: $i"
done
echo "======== Current environment dump FINISHING ========"

echo "======== Tarball initialization STARTING at $(TZ=GMT date) ========"
set -x
if [[ "X$CRAB3_RUNTIME_DEBUG" == "X" ]]; then
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
else
    echo "I am in runtime debug mode. I will not extract the sandbox"
fi
export PYTHONPATH=`pwd`/CRAB3.zip:`pwd`/WMCore.zip:$PYTHONPATH
set +x
echo "======== Tarball initialization FINISHING at $(TZ=GMT date) ========"
echo "==== Local directory contents dump STARTING ===="
echo "PWD: `pwd`"
for i in `ls`; do
  echo "== DIR: $i"
done
echo "==== Local directory contents dump FINISHING ===="
echo "======== CMSRunAnalysis.py STARTING at $(TZ=GMT date) ========"
echo "Now running the CMSRunAnalysis.py job in `pwd`..."
set -x
python CMSRunAnalysis.py -r "`pwd`" "$@"
jobrc=$?
set +x
echo "== The job had an exit code of $jobrc "
echo "======== CMSRunAnalysis.py FINISHING at $(TZ=GMT date) ========"

if [[ $jobrc == 68 ]]
then
  echo "WARNING: CMSSW encountered a malloc failure.  Logging ulimits:"
  sigterm
fi

if [[ $jobrc == 137 ]]
then
  echo "Job was killed. Check Postjob for kill reason."
  sigterm
fi

if [ -e scramOutput.log ]; then
  echo "==== SCRAM interaction log contents dump STARTING ===="
  cat scramOutput.log
  echo "==== SCRAM interaction log contents dump FINISHING ===="
else
  echo "ERROR: scramOutput.log does not exist."
fi

if [ ! -e wmcore_initialized ];
then
    echo "======== ERROR: Unable to initialize WMCore at $(TZ=GMT date) ========"
    sleep 20m
    exec sh ./DashboardFailure.sh 10043
fi

exit $jobrc

_
