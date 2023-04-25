#!/bin/bash

# if PUBLISHER_HOME is already defined, use it
if [ -v PUBLISHER_HOME ]
then
  echo "PUBLISHER_HOME already set to $PUBLISHER_HOME. Will use that"
else
  export PUBLISHER_HOME=/data/srv/Publisher  # where we run the Publisher and where Config is
  echo "Define environment for Publisher in $PUBLISHER_HOME"
fi

# if GH repositories location is not already defined, set a default
if ! [ -v GHrepoDir ]
then
  export GHrepoDir='/data/repos'
fi

# until we get a new openssl version, python3 requires this
export CRYPTOGRAPHY_ALLOW_OPENSSL_102=true

# cleanup the environment (needed in some cases when running interactively)
unset X509_USER_PROXY
unset X509_USER_CERT
unset X509_USER_KEY

# we only run from 'current' which points to current taskworker installation
# the following init.sh defines ${CRABTASKWORKER_ROOT} and ${CRABTASKWORKER_VERSION}
source ${PUBLISHER_HOME}/current/*/cms/crabtaskworker/*/etc/profile.d/init.sh

export PUBLISHER_ROOT=${CRABTASKWORKER_ROOT}
export PUBLISHER_VERSION=${CRABTASKWORKER_VERSION}
export CONDOR_CONFIG=/data/srv/condor_config
