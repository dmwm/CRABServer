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

# Note that the scheduler universe populate the .job.ad from 8.3.2 version.
# Before it was written by this script using condor_q (This command is expensive to Scheduler)
counter=0
while [[ (counter -lt 10) && (!(-e $_CONDOR_JOB_AD) || ($(cat $_CONDOR_JOB_AD | wc -l) -lt 3)) ]]; do
    sleep $((counter*3+1))
    let counter=counter+1
done

#Sourcing Remote Condor setup
source_script=`grep '^RemoteCondorSetup =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
if [ "X$source_script" != "X" ] && [ -e $source_script ];
then
    source $source_script
fi

#MAX_POST is set in TaskWorker configuration.
MAX_POST=20
MAX_POST_TMP=`grep '^CRAB_MaxPost =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
if [ "X$MAX_POST_TMP" != "X" ];
then
    MAX_POST=$MAX_POST_TMP
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

        TARBALL_NAME=TaskManagerRun.tar.gz
        tar xvfzm $TARBALL_NAME
        if [[ $? != 0 ]]
        then
                echo "Error: Unable to unpack the task manager runtime environment." >&2
                exit 3
        fi
        unzip CRAB3.zip
        ls -lah

        export TASKWORKER_ENV="1"
fi

# Recalculate the black / whitelist
if [ -e AdjustSites.py ];
then
    python2.6 AdjustSites.py
else
    echo 'Error: AdjustSites.py does not exist.' >&2
    exit 4
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
