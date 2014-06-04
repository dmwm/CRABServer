#!/bin/sh
source /etc/profile
echo "Beginning dag_boostrap_startup.sh at $(date)"
echo "On host: $(hostname)"
echo "At path: $(pwd)"
echo "As user: $(whoami)"
set -x

# Touch a bunch of files to make sure job doesn't go on hold for missing files
for FILE in $1.dagman.out RunJobs.dag RunJobs.dag.dagman.out RunJobs.dag.rescue.001 dbs_discovery.err dbs_discovery.out job_splitting.err job_splitting.out; do
    touch $FILE
done

# Add in python paths for UCSD and CERN submit hosts.  We'll get RemoteCondorSetup working again in the future.
export PATH="/opt/glidecondor/bin:/opt/glidecondor/sbin:/usr/local/bin:/bin:/usr/bin:/usr/bin:$PATH"
export PATH="/data/srv/glidecondor/bin:/data/srv/glidecondor/sbin:/usr/local/bin:/bin:/usr/bin:/usr/bin:$PATH"
export PYTHONPATH=/opt/glidecondor/lib/python:$PYTHONPATH
export PYTHONPATH=/data/srv/glidecondor/lib/python2.6:$PYTHONPATH
export LD_LIBRARY_PATH=/opt/glidecondor/lib:/opt/glidecondor/lib/condor:.:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/data/srv/glidecondor/lib:/data/srv/glidecondor/lib/condor:.:$LD_LIBRARY_PATH

mkdir -p retry_info

# Bootstrap the HTCondor environment

# Note that the scheduler universe does not populate the .job.ad file but the local
# universe does.  As CRAB3 switched to the scheduler universe, we must do this manually.
# We encountered transient condor_q failures when many tasks were submitted simultaneously;
# hence, the while loop was added.
counter=0
while [[ (counter -lt 10) && (!(-e .job.ad) || ($(cat .job.ad | wc -l) -lt 3)) ]]; do
    if [ "X$CONDOR_ID" != "X" ]; then
        condor_q $CONDOR_ID -l | grep -v '^$' > .job.ad
    fi
    sleep $((counter*3+1))
    let counter=counter+1
done
if [ "X$_CONDOR_JOB_AD" == "X" ];
then
    export _CONDOR_JOB_AD=.job.ad
fi

if [ "X$_CONDOR_JOB_AD" != "X" ];
then
    source_script=`grep '^RemoteCondorSetup =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
    if [ "X$source_script" != "X" ] && [ -e $source_script ];
    then
        source $source_script
    fi

fi

MAX_POST=20
if [ "X$_CONDOR_JOB_AD" != "X" ];
then
    MAX_POST_TMP=`grep '^CRAB_MaxPost =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
    if [ "X$MAX_POST_TMP" != "X" ];
    then
        MAX_POST=$MAX_POST_TMP
    fi
fi

# Recalculate the black / whitelist
if [ -e AdjustSites.py ];
then
  python2.6 AdjustSites.py
fi

# Bootstrap the runtime - we want to do this before DAG is submitted
# so all the children don't try this at once.
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
        unzip CRAB3.zip
	ls -lah

        export TASKWORKER_ENV="1"
fi

export _CONDOR_DAGMAN_LOG=$PWD/$1.dagman.out
export _CONDOR_MAX_DAGMAN_LOG=0

CONDOR_VERSION=`condor_version | head -n 1`
PROC_ID=`grep '^ProcId =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
CLUSTER_ID=`grep '^ClusterId =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
export CONDOR_ID="${CLUSTER_ID}.${PROC_ID}"
ls -lah
if [ "X" == "X$X509_USER_PROXY" ] || [ ! -e $X509_USER_PROXY ]; then
    echo "Failed to find a proxy at: $X509_USER_PROXY"
    EXIT_STATUS=5
elif [ ! -r $X509_USER_PROXY ]; then
    echo "The proxy is unreadable for some reason"
    EXIT_STATUS=6
else
    # Re-nice the process so, even when we churn through lots of processes, we never starve the schedd or shadows for cycles.
    exec nice -n 19 condor_dagman -f -l . -Lockfile $PWD/$1.lock -AutoRescue 1 -DoRescueFrom 0 -MaxPre 20 -MaxIdle 200 -MaxPost $MAX_POST -Dag $PWD/$1 -Dagman `which condor_dagman` -CsdVersion "$CONDOR_VERSION" -debug 4 -verbose
    EXIT_STATUS=$?
fi
exit $EXIT_STATUS

