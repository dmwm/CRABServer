#! /bin/bash
# This script mean to run inside crabtaskworker image.
# Hardcoded the DATAFILES_WORKDIR, RUNTIME_WORKDIR, WMCOREDIR, CRABSERVERDIR,
# and DATA_DIR to TaskWorker's deployment convention
# See description of variables in cicd/crabtaskworker_pypi/build_data_files.sh

set -euo pipefail
#set -x

# use "convention path" of repos to build datafiles inside /data/build
DATAFILES_WORKDIR=/data/build/datafiles_dir
RUNTIME_WORKDIR=/data/build/make_runtime
WMCOREDIR=/data/repos/WMCore
CRABSERVERDIR=/data/repos/CRABServer

# building data files
echo "Building data files from source."
mkdir -p "${DATAFILES_WORKDIR}" "${RUNTIME_WORKDIR}"
pushd "${CRABSERVERDIR}"
echo "Running build_data_files.sh script from $CRABSERVERDIR"
export DATAFILES_WORKDIR
export RUNTIME_WORKDIR
export WMCOREDIR
export CRABSERVERDIR
bash cicd/crabtaskworker_pypi/build_data_files.sh
popd

# copy new data files to data files path
DATA_DIR=/data/srv/current/lib/python/site-packages/data
echo "Cleaning ${DATA_DIR}"
rm -rf "${DATA_DIR:?}"/*
echo "Copy new data files from ${DATAFILES_WORKDIR} to ${DATA_DIR}"
cp -rp "${DATAFILES_WORKDIR}"/data/* "${DATA_DIR}"
