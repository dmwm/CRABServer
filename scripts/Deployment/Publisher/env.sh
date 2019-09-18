#!/bin/bash

# if PUBLISHER_HOME is already defined, use it
if [ -v PUBLISHER_HOME ]
then
  echo "PUBLISHER_HOME already set to $PUBLISHER_HOME. Will use that"
else
  export PUBLISHER_HOME=/data/srv/Publisher  # where we run the Publisher and where Config is
  echo "Define environment for Publisher in $PUBLISHER_HOME"
fi

# we only run from 'current' which points to current taskworker installation

source ${PUBLISHER_HOME}/current/*/cms/crabtaskworker/*/etc/profile.d/init.sh

export PUBLISHER_ROOT=${CRABTASKWORKER_ROOT}
export PUBLISHER_VERSION=${CRABTASKWORKER_VERSION} 

export CONDOR_CONFIG=/data/srv/condor_config
