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

# Add in python paths for UCSD submit hosts
export PATH="/opt/glidecondor/bin:/opt/glidecondor/sbin:/usr/local/bin:/bin:/usr/bin:/usr/bin:$PATH"
export PYTHONPATH=/opt/glidecondor/lib/python:$PYTHONPATH
export LD_LIBRARY_PATH=/opt/glidecondor/lib:/opt/glidecondor/lib/condor:.:$LD_LIBRARY_PATH

# Bootstrap the HTCondor environment
if [ "X$_CONDOR_JOB_AD" != "X" ];
then
    source_script=`grep '^RemoteCondorSetup =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
    if [ "X$source_script" != "X" ] && [ -e $source_script ];
    then
        source $source_script
    fi

fi

# Recalculate the black / whitelist
if [ -e AdjustSites.py ];
then
  python AdjustSites.py
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
    # There used to be -Suppress_notification here. Why?
    exec condor_dagman -f -l . -Lockfile $PWD/$1.lock -AutoRescue 1 -DoRescueFrom 0 -MaxIdle 100 -MaxPost 20 -Dag $PWD/$1 -Dagman `which condor_dagman` -CsdVersion "$CONDOR_VERSION" -debug 2 -verbose
    EXIT_STATUS=$?
fi
exit $EXIT_STATUS

