#!/bin/sh

# Touch a bunch of files to make sure job doesn't go on hold for missing files
master_dag.dagman.out
master_dag.rescue.001
RunJobs.dag
RunJobs.dag.dagman.out
RunJobs.dag.rescue.001
dbs_discovery.err
dbs_discovery.out
job_splitting.err
job_splitting.out

# Bootstrap the HTCondor environment
if [ "X$_CONDOR_JOB_AD" != "X" ];
then
    source_script=`grep '^RemoteCondorSetup =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
    if [ "X$source_script" != "X" ] && [ -e $source_script ];
    then
        source $source_script
    fi
fi

export _CONDOR_DAGMAN_LOG=$PWD/master_dag.dagman.out
export _CONDOR_MAX_DAGMAN_LOG=0

CONDOR_VERSION=`condor_version | head -n 1`

echo "condor_dagman -f -l . -Lockfile $PWD/master_dag.lock -AutoRescue 1 -DoRescueFrom 0 -Dag $PWD/master_dag -Suppress_notification -CsdVersion $CONDOR_VERSION -Dagman `which condor_dagman`"
exec condor_dagman -f -l . -Lockfile $PWD/master_dag.lock -AutoRescue 1 -DoRescueFrom 0 -Dag $PWD/master_dag -Suppress_notification -Dagman `which condor_dagman` -CsdVersion "$CONDOR_VERSION"

