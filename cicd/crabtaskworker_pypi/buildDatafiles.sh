#!/bin/bash
# Build data files of TaskWorker.
# This script needs
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

# get absolute path
RUNTIME_WORKDIR="$(realpath "${RUNTIME_WORKDIR}")"
DATAFILES_WORKDIR="$(realpath "${DATAFILES_WORKDIR}")"
CRABSERVERDIR="$(realpath "${CRABSERVERDIR}")"
WMCOREDIR="$(realpath "${WMCOREDIR}")"

# cleanup $DATAFILES_WORKDIR
rm -rf "${DATAFILES_WORKDIR}"/data
mkdir -p "${DATAFILES_WORKDIR}"

# build tar files
# passing args
export RUNTIME_WORKDIR
export CRABSERVERDIR
export WMCOREDIR
# assume new_htcondor_make_runtime.sh live in the same directory of this script.
bash "${SCRIPT_DIR}"/buildTWTarballs.sh
# build script files
pushd "${CRABSERVERDIR}"
python3 setup.py install_system -s TaskWorker --prefix="${DATAFILES_WORKDIR}"
popd
# copy tar files to the same directory as script files
cp "${RUNTIME_WORKDIR}/CMSRunAnalysis.tar.gz" \
   "${RUNTIME_WORKDIR}/TaskManagerRun.tar.gz" \
   "${DATAFILES_WORKDIR}/data"

# remove unuse lib dir from `install_system`
rm -rf "${DATAFILES_WORKDIR:?}/lib"
