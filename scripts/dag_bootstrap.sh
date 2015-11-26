#!/bin/bash

# 
# This script bootstraps the WMCore environment
#
set -x
echo "Beginning dag_bootstrap.sh (stdout)"
echo "Beginning dag_bootstrap.sh (stderr)" 1>&2
if [ "X$TASKWORKER_ENV" = "X" -a ! -e CRAB3.zip ]
then

	command -v python2.6 > /dev/null
	rc=$?
	if [[ $rc != 0 ]]
	then
		echo "Error: Python2.6 isn't available on `hostname`." >&2
		echo "Error: bootstrap execution requires python2.6" >&2
		exit 1
	else
		echo "I found python2.6 at.."
		echo `which python2.6`
	fi

	if [ "x$CRAB3_VERSION" = "x" ]; then
		TARBALL_NAME=TaskManagerRun.tar.gz
	else
		TARBALL_NAME=TaskManagerRun-$CRAB3_VERSION.tar.gz
	fi

	if [[ "X$CRAB_TASKMANAGER_TARBALL" == "X" ]]; then
		CRAB_TASKMANAGER_TARBALL="http://hcc-briantest.unl.edu/$TARBALL_NAME"
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
    	
	tar xvfzm TaskManagerRun.tar.gz
	if [[ $? != 0 ]]
	then
		echo "Error: Unable to unpack the task manager runtime environment." >&2
		exit 4
	fi
    ls -lah

        export TASKWORKER_ENV="1"
fi

export PYTHONPATH=$PWD:$PWD/CRAB3.zip:$PYTHONPATH

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

# If at CERN, try to bootstrap the UI:
if [ "$1" == "POSTJOB" ];
then
if [ -e /afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh ];
then
    . /afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh
fi
fi

export PATH="/opt/glidecondor/bin:/opt/glidecondor/sbin:/usr/local/bin:/bin:/usr/bin:/usr/bin:$PATH"
export PATH="/data/srv/glidecondor/bin:/data/srv/glidecondor/sbin:/usr/local/bin:/bin:/usr/bin:/usr/bin:$PATH"
export PYTHONPATH=/opt/glidecondor/lib/python:$PYTHONPATH
export PYTHONPATH=/data/srv/glidecondor/lib/python2.6:$PYTHONPATH
export LD_LIBRARY_PATH=/opt/glidecondor/lib:/opt/glidecondor/lib/condor:.:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/data/srv/glidecondor/lib:/data/srv/glidecondor/lib/condor:.:$LD_LIBRARY_PATH
export PYTHONUNBUFFERED=1
echo "Printing current environment..."
env
if [ "X$_CONDOR_JOB_AD" != "X" ]; then
  echo "Printing current job ad..."
  cat $_CONDOR_JOB_AD
fi
echo "Now running the job in `pwd`..."
exec nice -n 19 python2.6 -m TaskWorker.TaskManagerBootstrap "$@"
