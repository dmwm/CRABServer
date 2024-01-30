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
  echo "Logging work directory file sizes:"
  ls -lnh
  if [ ! -e logCMSSWSaved.txt ];
  then
    python3 -c "import CMSRunAnalysis; CMSRunAnalysis.logCMSSW()"
  fi
}

#
echo "======== CMSRunAnalysis.sh STARTING at $(TZ=GMT date) ========"
echo "Local time : $(date)"
echo "Current system : $(uname -a)"
echo "Current processor: $(cat /proc/cpuinfo |grep name|sort|uniq)"

source ./submit_env.sh

# from ./submit_env.sh
setup_cmsset

# from ./submit_env.sh
setup_python_comp

echo "==== Make sure $HOME is defined ===="
export HOME=${HOME:-$PWD}

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
$pythonCommand CMSRunAnalysis.py "$@"
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
fi

exit $jobrc

