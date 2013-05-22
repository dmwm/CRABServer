#!/bin/bash

# 
# This script bootstraps the WMCore environment
#

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

	curl http://hcc-briantest.unl.edu/TaskManagerRun.tar.gz > TaskManagerRun.tar.gz
	if [[ $? != 0 ]]
	then
		echo "Error: Unable to download the task manager runtime environment." >&2
		exit 3
	fi
	tar xvfzm TaskManagerRun.tar.gz
	if [[ $? != 0 ]]
	then
		echo "Error: Unable to unpack the task manager runtime environment." >&2
		exit 4
	fi
	rm TaskManagerRun.tar.gz

        export TASKWORKER_ENV="1"
fi

export PYTHONPATH=$PWD:$PWD/CRAB3.zip:$PYTHONPATH
# This is for development-only, due to the rapid rate we're updating the TaskWorker files.
# TODO: Remove the below lines before the first release.
export PYTHONPATH=~/projects/CAFTaskWorker/src/python:$PYTHONPATH

echo "Checking for classad hack at path $HOME/classad_hack_library on host ..."
if [ -d ~/classad_hack_library ]; then
    echo "Engaging classad hack in dag_bootstrap.sh"
    export PYTHONPATH=$PYTHONPATH:~/classad_hack_library
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:~/classad_hack_library
    echo "pythonpath: $PYTHONPATH"
    echo "library path: $LD_LIBRARY_PATH"
fi

if [[ "x$X509_USER_PROXY" = "x" ]]; then
    export X509_USER_PROXY=$(pwd)/user.proxy
fi

export PATH="/opt/glidecondor/bin:/opt/glidecondor/sbin:/usr/local/bin:/bin:/usr/bin:/usr/bin:$PATH"
env
echo "Now running the job in `pwd`..."
exec python2.6 -m TaskWorker.TaskManagerBootstrap "$@"

