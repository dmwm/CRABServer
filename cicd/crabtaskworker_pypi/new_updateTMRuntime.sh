#! /bin/bash
# This script mean to run inside crabtaskworker image.
# Hardcoded the INSTALL_DIR, RUNTIME_DIR, WMCOREDIR, CRABSERVERDIR,
# and DATA_DIR to TaskWorker's deployment convention
# See description of variables in cicd/crabtaskworker_pypi/build_data_files.sh

set -euo pipefail
#set -x

# use "convention path" of repos to build datafiles inside /data/build
INSTALL_DIR=/data/build/install_dir
RUNTIME_DIR=/data/build/make_runtime
WMCOREDIR=/data/repos/WMCore
CRABSERVERDIR=/data/repos/CRABServer

# building data files
echo "Building data files from source."
mkdir -p "${INSTALL_DIR}" "${RUNTIME_DIR}"
export INSTALL_DIR RUNTIME_DIR WMCOREDIR CRABSERVERDIR
pushd "${CRABSERVERDIR}"
echo "Running build_data_files.sh script from $CRABSERVERDIR"
bash cicd/crabtaskworker_pypi/build_data_files.sh
popd

# backup container's data files
DATA_DIR=/data/srv/current/lib/python/site-packages/data
BACKUP_DIR="$(realpath "${DATA_DIR}/../../../")"
ORIGINAL_DATA_FILES_PATH=$BACKUP_DIR/Original_data_files.tar.gz
if [[ ! -f "${ORIGINAL_DATA_FILES_PATH}" ]]; then
    echo "Backing up container's data files to $ORIGINAL_DATA_FILES_PATH"
    tar -zcf "${ORIGINAL_DATA_FILES_PATH}" -C "${DATA_DIR}/../" data
fi

# copy new data files to data files path
echo "Cleaning ${DATA_DIR}"
rm -rf "${DATA_DIR:?}"/*
echo "Copy new data files from ${INSTALL_DIR} to ${DATA_DIR}"
cp -rp "${INSTALL_DIR}"/data/* "${DATA_DIR}"
