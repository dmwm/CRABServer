#!/bin/sh

# Bootstrap the HTCondor environment
if [ "X$_JOB_AD" != "X" ];
then
    source_script=`grep '^RemoteCondorSetup =' $_JOB_AD | tr -d '"' | awk '{print $NF;}'`
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

