#!/bin/bash

# if TASKWORKER_HOME is already defined, use it
if [ -v TASKWORKER_HOME ]
then
  echo "TASKWORKER_HOME already set to $TASKWORKER_HOME. Will use that"
else
  export TASKWORKER_HOME=/data/srv/TaskManager  # where we run the TASKWORKER and where Config is
  echo "Define environment for TASKWORKER in $TASKWORKER_HOME"
fi

# if GH repositories location is not already defined, set a default
if ! [ -v GHrepoDir ]
then
  export GHrepoDir='/data/repos'
fi

# cleanup the environment (needed in some cases when running interactively)
unset X509_USER_PROXY
unset X509_USER_CERT
unset X509_USER_KEY

# we only run from 'current' which points to current taskworker installation
# the following init.sh defines ${CRABTASKWORKER_ROOT} and ${CRABTASKWORKER_VERSION}
source ${TASKWORKER_HOME}/current/*/cms/crabtaskworker/*/etc/profile.d/init.sh

export TASKWORKER_ROOT=${CRABTASKWORKER_ROOT}
export CRABTASKWORKER_ROOT
export TASKWORKER_VERSION=${CRABTASKWORKER_VERSION}
export CONDOR_CONFIG=/data/srv/condor_config
