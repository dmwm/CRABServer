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

condor_dagman -Dag master_dag

