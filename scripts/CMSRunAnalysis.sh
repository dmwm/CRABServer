#!/bin/bash
exec 2>&1

function DashboardFailure {
    if [ -f ./DashboardFailure.sh ]; then
        exec sh ./DashboardFailure.sh $1
    else
        exit $1
    fi
}

sigterm() {
  echo "ERROR: Job was killed. Logging ulimits:"
  ulimit -a
  echo "Logging free memory info:"
  free -m
  echo "Logging disk usage:"
  df -h
  echo "Logging disk usage in directory:"
  du -h
  echo "Logging work directory file sizes:"
  ls -lnh
  DashboardFailure 50669
  if [ ! -e logCMSSWSaved.txt ];
  then
    python -c "import CMSRunAnalysis; logCMSSW()"
  fi
}

#
echo "======== CMSRunAnalysis.sh STARTING at $(TZ=GMT date) ========"
echo "Local time : $(date)"
echo "Current system : $(uname -a)"
echo "Current processor: $(cat /proc/cpuinfo |grep name|sort|uniq)"

### source the CMSSW stuff using either OSG or LCG style entry env. or CVMFS
echo "======== CMS environment load starting at $(TZ=GMT date) ========"
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
echo -e "========  CMS environment load finished at $(TZ=GMT date) ========\n"

echo "==== Python discovery STARTING ===="
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

echo "==== Python discovery FINISHED at $(TZ=GMT date) ===="

echo "======== Current environment dump STARTING ========"
for i in `env`; do
  echo "== ENV: $i"
done
echo "======== Current environment dump FINISHING ========"

echo "======== Tarball initialization STARTING at $(TZ=GMT date) ========"
set -x
if [[ "X$CRAB3_RUNTIME_DEBUG" == "X" ]]; then
    if [[ $CRAB_RUNTIME_TARBALL == "local" ]]; then
        # Tarball was shipped with condor
        tar xmf CMSRunAnalysis.tar.gz || exit 10042
    else
        # Allow user to override the choice
        curl $CRAB_RUNTIME_TARBALL | tar xm || exit 10042
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
  echo "WARNING: CMSSW encountered a malloc failure. Logging ulimits:"
  sigterm
fi

if [[ $jobrc == 137 ]]
then
  echo "Job was killed. Check Postjob for kill reason."
  sigterm
fi


if [ ! -e wmcore_initialized ];
then
    echo "======== ERROR: Unable to initialize WMCore at $(TZ=GMT date) ========"
    DashboardFailure 10043
fi

exit $jobrc

