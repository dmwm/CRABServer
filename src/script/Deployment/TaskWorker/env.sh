#!/bin/bash

# if TASKWORKER_HOME is already defined, use it
if [ -v TASKWORKER_HOME ]
then
  echo "TASKWORKER_HOME already set to $TASKWORKER_HOME. Will use that"
else
  export TASKWORKER_HOME=/data/srv/TaskManager  # where we run the TASKWORKER and where Config is
  echo "Define environment for TASKWORKER in $TASKWORKER_HOME"
fi

# we only run from 'current' which points to current taskworker installation
source ${TASKWORKER_HOME}/current/*/cms/crabtaskworker/*/etc/profile.d/init.sh

export CRABTASKWORKER_ROOT
export CRABTASKWORKER_VERSION
export CONDOR_CONFIG=/data/srv/condor_config
export GHrepoDir=/data/hostdisk/repos
