#!/bin/bash
# This scripts installs in the container a TaskWorker image from rpm repositories
# the same image can be used to run different services which use the same code bas
# Currently those services include TaskWorker and Publisher_*

# The script is meant to be used inside the file CRABServer/Docker/Dockerfile
env
set -x
echo starting

touch /data/srv/condor_config

export RELEASE=$TW_VERSION 

export MYTESTAREA=/data/srv/TaskManager/$RELEASE
export SCRAM_ARCH=slc7_amd64_gcc630

# string after the underscore (_) tells which branch was used to build rpms
export REPO=comp.crab_master

export verbose=true
echo 'Installation'

mkdir -p $MYTESTAREA
wget -O $MYTESTAREA/bootstrap.sh http://cmsrep.cern.ch/cmssw/repos/bootstrap.sh
sh $MYTESTAREA/bootstrap.sh -architecture $SCRAM_ARCH -path $MYTESTAREA -repository $REPO setup

cd /data/srv/TaskManager
$RELEASE/common/cmspkg -a $SCRAM_ARCH upgrade
$RELEASE/common/cmspkg -a $SCRAM_ARCH update
$RELEASE/common/cmspkg -a $SCRAM_ARCH install cms+crabtaskworker+$RELEASE || echo "Installation failed. Please check log lines above for details" && exit 1

cp $MYTESTAREA/$SCRAM_ARCH/cms/crabtaskworker/$RELEASE/data/script/Deployment/Publisher/{start.sh,env.sh,stop.sh,} /data/srv/Publisher/
cp $MYTESTAREA/$SCRAM_ARCH/cms/crabtaskworker/$RELEASE/data/script/Deployment/TaskWorker/{start.sh,env.sh,stop.sh,updateTMRuntime.sh} /data/srv/TaskManager/

ln -s $RELEASE current
ln -s /data/srv/TaskManager/current /data/srv/Publisher/current

set +x

