#!/bin/bash

# 
# This script bootstraps the WMCore environment
#
set -x
echo "Beginning dag_bootstrap.sh (stdout)"
echo "Beginning dag_bootstrap.sh (stderr)" 1>&2
if [ "X$TASKWORKER_ENV" = "X" -a ! -e CRAB3.zip ]
then

	command -v python > /dev/null
	rc=$?
	if [[ $rc != 0 ]]
	then
		echo "Error: Python isn't available on `hostname`." >&2
		echo "Error: bootstrap execution requires python" >&2
		exit 1
	else
		echo "I found python at.."
		echo `which python`
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
    	
	tar xvfm TaskManagerRun.tar.gz
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

rhrel_str=`cat /etc/redhat-release`
rhrel_str=${rhrel_str##*release }
os_ver=${rhrel_str%%.*}
curl_path="/cvmfs/cms.cern.ch/slc${os_ver}_amd64_gcc700/external/curl/7.59.0"
libcurl_path="${curl_path}/lib"
source ${curl_path}/etc/profile.d/init.sh

export PATH="/opt/glidecondor/bin:/opt/glidecondor/sbin:/usr/local/bin:/bin:/usr/bin:/usr/bin:$PATH"
export PATH="/data/srv/glidecondor/bin:/data/srv/glidecondor/sbin:/usr/local/bin:/bin:/usr/bin:/usr/bin:$PATH"
export PYTHONPATH=/opt/glidecondor/lib/python:$PYTHONPATH
export LD_LIBRARY_PATH=/opt/glidecondor/lib:/opt/glidecondor/lib/condor:.:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/data/srv/glidecondor/lib:/data/srv/glidecondor/lib/condor:.:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=$libcurl_path:$LD_LIBRARY_PATH

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
exec nice -n 19 python -m TaskWorker.TaskManagerBootstrap "$@"
