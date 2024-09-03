#!/bin/bash
# This script mean to run inside crabtaskworker image.
# Hardcoded the DATAFILES_WORKDIR, RUNTIME_WORKDIR, WMCOREDIR, CRABSERVERDIR,
# and DATA_DIR to TaskWorker's deployment convention
# See description of variables in cicd/crabtaskworker_pypi/buildDataFiles.sh

set -euo pipefail
if [[ -n ${TRACE+x} ]]; then
    set -x
    export TRACE
fi

# use "convention path" of repos to build datafiles inside /data/build
DATAFILES_WORKDIR=/data/build/datafiles_dir
RUNTIME_WORKDIR=/data/build/make_runtime
WMCOREDIR=/data/repos/WMCore
CRABSERVERDIR=/data/repos/CRABServer

# building data files
echo "Building data files from source."
mkdir -p "${DATAFILES_WORKDIR}" "${RUNTIME_WORKDIR}"
pushd "${CRABSERVERDIR}"
echo "Running buildDatafiles.sh script from $CRABSERVERDIR"
export DATAFILES_WORKDIR
export RUNTIME_WORKDIR
export WMCOREDIR
export CRABSERVERDIR
# the buildDatafiles is too verbose, pipe it to file instead if not TRACE
if [[ -n ${TRACE+x} ]]; then
    bash cicd/crabtaskworker_pypi/buildDatafiles.sh
else
    logpath=/tmp/buildDatafiles-$(date +"%Y%m%d-%H%M%S").log
    bash cicd/crabtaskworker_pypi/buildDatafiles.sh &> "${logpath}"
    echo "buildDatafiles.sh log path: ${logpath}"
fi
popd

# copy new data files to data files path
DATA_DIR=/data/srv/current/lib/python/site-packages/data
echo "Cleaning ${DATA_DIR}"
rm -rf "${DATA_DIR:?}"/*
echo "Copy new data files from ${DATAFILES_WORKDIR} to ${DATA_DIR}"
cp -rp "${DATAFILES_WORKDIR}"/data/* "${DATA_DIR}"
