#!/bin/bash

# 
# This script bootstraps the WMCore environment
#
# wrap the whole script in {} in order to redirect stdout/err to a file
# trick from https://stackoverflow.com/a/315113
# and set the exit status $? to the exit code of the last command to exit non-zero
# taken from https://unix.stackexchange.com/a/73180

set -o pipefail
{
set -x
echo "Beginning dag_bootstrap.sh (stdout)"
echo "Beginning dag_bootstrap.sh (stderr)" 1>&2

# in case of POSTJOB and ASO-Rucio check if we want to exit immediately w/o running python
# arguments to this script come from Dagman description file and are e.g. listed in
# https://github.com/dmwm/CRABServer/blob/44564702517657f69da9d667db70105da0360ae3/src/python/TaskWorker/Actions/DagmanCreator.py#L55
# let's give names to the ones we use here
scriptKind=$1
crabId=$7
retry=$4
outputDest=$9

if [[ $scriptKind == "POSTJOB" ]] && [[ $outputDest =~ "/rucio/" ]] ; then
  pjlog=postjob.$crabId.$retry.txt
  defers=`grep -c DEFERRING $pjlog`  # there's one defer every 30min, so this counts time !
  prob=1
  [[ $defers -ge 8 ]] && prob=$(($defers/4))  # see https://github.com/dmwm/CRABServer/issues/9079
  test=`shuf -i 1-$prob -n 1`  # a random integer in [`1...$prob]
  # the PostJob python script is only run if the random draw equals to 1
  # for the first 4 hours, $defers is < 8 and $prob is always 1 so $test is always 1
  # after 4 hours  we give it a 4/defers probability to actually run the python check
  if [ $test -ne 1 ] ; then
    echo `date` "dag_boostrap.sh DEFERRING. PostJob will run again after 30 min" >> $pjlog
    exit 4  # tells Dagman to re-run me after DEFER time indicated in Dagman file
  fi
fi

source /etc/os-release
OS_Version=`echo $VERSION_ID | cut -d. -f1`  # e.g. 7 or 9

if [ "$OS_Version" = "7" ]
then
  export PYTHONPATH=$PYTHONPATH:/cvmfs/cms.cern.ch/rucio/x86_64/slc7/py3/current/lib/python3.6/site-packages/
  export PYTHONPATH=$PYTHONPATH:/data/srv/pycurl3/7.44.1
  curl_path="/cvmfs/cms.cern.ch/slc7_amd64_gcc700/external/curl/7.59.0"
  source ${curl_path}/etc/profile.d/init.sh
elif [ "$OS_Version" = "9" ]
then
  export PYTHONPATH=$PYTHONPATH:/cvmfs/cms.cern.ch/rucio/x86_64/rhel9/py3/current/lib/python3.9/site-packages/
  # alma9 comes with working curl and pycurl
else
  echo " WE DO NOT SUPPORT OS_Version = $OS_Version "
  exit 1
fi

if [ "X$TASKWORKER_ENV" = "X" -a ! -e CRAB3.zip ]
then

	command -v python3 > /dev/null
	rc=$?
	if [[ $rc != 0 ]]
	then
		echo "Error: Python3 isn't available on `hostname`." >&2
		echo "Error: bootstrap execution requires python3" >&2
		exit 1
	else
		echo "I found python3 at.."
		echo `which python3`
	fi

	if [ "x$CRAB3_VERSION" = "x" ]; then
		TARBALL_NAME=TaskManagerRun.tar.gz
	else
		TARBALL_NAME=TaskManagerRun-$CRAB3_VERSION.tar.gz
	fi
    
	if [[ "X$CRAB_TASKMANAGER_TARBALL" != "Xlocal" ]]; then
		# pass, we'll just use that value
		echo "Downloading tarball from $CRAB_TASKMANAGER_TARBALL"
		curl $CRAB_TASKMANAGER_TARBALL > TaskManagerRun.tar.gz
		if [[ $? != 0 ]]
		then
			echo "Error: Unable to download the task manager runtime environment." >&2
			exit 3
		fi
	else
		echo "Using tarball shipped within condor"
	fi
    	
	tar xvfm TaskManagerRun.tar.gz
	if [[ $? != 0 ]]
	then
		echo "Error: Unable to unpack the task manager runtime environment." >&2
		exit 4
	fi
    ls -lah

        export TASKWORKER_ENV="1"
fi

export PYTHONPATH=$PWD:$PWD/CRAB3.zip:$PWD/WMCore.zip:$PYTHONPATH

#if [[ "x$X509_USER_PROXY" = "x" ]]; then
#    export X509_USER_PROXY=$(pwd)/user.proxy
#fi
if [ "x" == "x$X509_USER_PROXY" ] || [ ! -e $X509_USER_PROXY ]; then
    echo "ERROR: Couldn't find a valid proxy at $X509_USER_PROXY"
    echo "Got the following environment variables that may help:"
    env | grep -i proxy
    env | grep 509
    echo "Got the following things in the job ad that may help:"
    if [ "X$_CONDOR_JOB_AD" != "X" ]; then
      cat $_CONDOR_JOB_AD | sort
    fi
    exit 5
fi

# Bootstrap the HTCondor environment
if [ "X$_CONDOR_JOB_AD" != "X" ];
then
    source_script=`grep '^RemoteCondorSetup =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
    if [ "X$source_script" != "X" ] && [ -e $source_script ];
    then
        source $source_script
    fi
fi


export PATH="usr/local/bin:/bin:/usr/bin:/usr/bin:$PATH"
export LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH

# for automatic splitting, DagmanCreator needs HOSTNAME env.var. to be set
# see https://github.com/dmwm/CRABServer/issues/7652
# possibly a new requirement since we upgraded to condor 10
export HOSTNAME

export PYTHONUNBUFFERED=1
echo "Printing current environment..."

srcname=$0
env > ${srcname%.sh}.env

env
if [ "X$_CONDOR_JOB_AD" != "X" ]; then
  echo "Printing current job ad..."
  cat $_CONDOR_JOB_AD
fi
echo "Now running the job in `pwd`..."
exec nice -n 19 python3 -m TaskWorker.TaskManagerBootstrap "$@"
} 2>&1 | tee "dag_bootstrap_${crabId}_${retry}.out"
