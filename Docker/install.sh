#!/bin/bash

env
set -x
echo starting

touch /data/srv/condor_config

export RELEASE=$TW_VERSION 

export MYTESTAREA=/data/srv/TaskManager/$RELEASE
export SCRAM_ARCH=slc7_amd64_gcc630
     
export REPO=comp.crab3

export verbose=true
echo 'Installation'

mkdir -p $MYTESTAREA
wget -O $MYTESTAREA/bootstrap.sh http://cmsrep.cern.ch/cmssw/repos/bootstrap.sh
sh $MYTESTAREA/bootstrap.sh -architecture $SCRAM_ARCH -path $MYTESTAREA -repository $REPO setup

cd /data/srv/TaskManager
$RELEASE/common/cmspkg -a $SCRAM_ARCH upgrade
$RELEASE/common/cmspkg -a $SCRAM_ARCH update
$RELEASE/common/cmspkg -a $SCRAM_ARCH install cms+crabtaskworker+$RELEASE

cp $MYTESTAREA/$SCRAM_ARCH/cms/crabtaskworker/$RELEASE/data/script/Deployment/Publisher/{start.sh,env.sh,stop.sh,} /data/srv/Publisher/
cp $MYTESTAREA/$SCRAM_ARCH/cms/crabtaskworker/$RELEASE/data/script/Deployment/TaskWorker/{start.sh,env.sh,stop.sh,updateTMRuntime.sh} /data/srv/TaskManager/

ln -s $RELEASE current
ln -s /data/srv/TaskManager/current /data/srv/Publisher/current

set +x

