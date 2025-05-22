#!/bin/bash
# Build data files of TaskWorker.
# This script needs:
#   - RUNTIME_WORKDIR:   Working directory to build tar files
#   - DATAFILES_WORKDIR: Working directory to build data files
#   - CRABSERVERDIR:     CRABServer repository path.
#   - WMCOREDIR:         WMCore repository path.
#
# Note that in Dockerfile and updateTMRuntime.sh, it set DATAFILES_WORKDIR to somewhere and copy built files to the path of data files later.
#
# Exemple for running in local machine (use default RUNTIME_WORKDIR and DATAFILES_WORKDIR, and enable trace):
# CRABSERVERDIR=./ WMCOREDIR=../WMCore TRACE=true bash cicd/crabtaskworker/buildDatafiles.sh

set -euo pipefail
if [[ -n ${TRACE+x} ]]; then
    set -x
    export TRACE
fi
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

RUNTIME_WORKDIR="${RUNTIME_WORKDIR:-./make_runtime}"
DATAFILES_WORKDIR="${DATAFILES_WORKDIR:-./data_files}"
CRABSERVERDIR="${CRABSERVERDIR:-./}"
WMCOREDIR="${WMCOREDIR:-./WMCore}"

# Get absolute paths
RUNTIME_WORKDIR="$(realpath "${RUNTIME_WORKDIR}")"
DATAFILES_WORKDIR="$(realpath "${DATAFILES_WORKDIR}")"
CRABSERVERDIR="$(realpath "${CRABSERVERDIR}")"
WMCOREDIR="$(realpath "${WMCOREDIR}")"

# Cleanup $DATAFILES_WORKDIR
rm -rf "${DATAFILES_WORKDIR}/data"
mkdir -p "${DATAFILES_WORKDIR}/data"

# Build tar files
export RUNTIME_WORKDIR
export CRABSERVERDIR
export WMCOREDIR
# assume new_htcondor_make_runtime.sh is in the same directory
bash "${SCRIPT_DIR}/buildTWTarballs.sh"

# Install the package in a temporary location to extract data files
TMP_INSTALL_DIR="$(mktemp -d)"
pushd "${CRABSERVERDIR}"
python3 setup.py install_system -s TaskWorker --prefix="${TMP_INSTALL_DIR}" --root=/
popd

# Copy installed package data files to DATAFILES_WORKDIR
PKG_DATA_DIR="${TMP_INSTALL_DIR}/lib/python*/site-packages/crabserver"
if [[ -d "${PKG_DATA_DIR}" ]]; then
    # Copy scripts
    cp -r "${PKG_DATA_DIR}/scripts" "${DATAFILES_WORKDIR}/data/"
    # Copy web files (css, html, script)
    for dir in css html script; do
        if [[ -d "${PKG_DATA_DIR}/${dir}" ]]; then
            cp -r "${PKG_DATA_DIR}/${dir}" "${DATAFILES_WORKDIR}/data/"
        fi
    done
else
    echo "ERROR: Package data not found in ${PKG_DATA_DIR}" >&2
    exit 1
fi

# Copy tar files
cp "${RUNTIME_WORKDIR}/CMSRunAnalysis.tar.gz" \
   "${RUNTIME_WORKDIR}/TaskManagerRun.tar.gz" \
   "${DATAFILES_WORKDIR}/data/"

# Cleanup temporary install
rm -rf "${TMP_INSTALL_DIR}"
rm -rf "${DATAFILES_WORKDIR:?}/lib"

echo "Data files successfully built in ${DATAFILES_WORKDIR}/data"