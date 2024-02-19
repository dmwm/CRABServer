#! /bin/bash
# Build data files of TaskWorker.
# This script needs
#   - RUNTIME_DIR:   Working directory to build tar files
#   - INSTALL_DIR:   data files directory
#   - CRABSERVERDIR: CRABServer repository path.
#   - WMCOREDIR:     WMCore repository path.
# Note that
#   - in Dockerfile and updateTMRuntime.sh, it set INSTALL_DIR to somewhere and copy built files to the path of data-files later.
#   - WMCOREDIR is used inside new_htcondor_make_runtime.sh

set -euo pipefail
TRACE=${TRACE:-}
if [[ -n $TRACE ]]; then
    set +x
fi

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

RUNTIME_DIR="${RUNTIME_DIR:-./make_runtime}"
INSTALL_DIR="${INSTALL_DIR:-./install_dir}"
CRABSERVERDIR="${CRABSERVER_DIR:-.}"

# get absolute path
RUNTIME_DIR="$(realpath "${RUNTIME_DIR}")"
INSTALL_DIR="$(realpath "${INSTALL_DIR}")"
CRABSERVERDIR="$(realpath "${CRABSERVERDIR}")"

# cleanup $INSTALL_DIR
rm -rf "${INSTALL_DIR}"
mkdir -p "${INSTALL_DIR}"

# build tar files
# assume new_htcondor_make_runtime.sh live in the same directory of
# this script.
bash "${SCRIPT_DIR}/new_htcondor_make_runtime.sh"
# build script files
pushd "${CRABSERVERDIR}"
python3 setup.py install_system -s TaskWorker --prefix="${INSTALL_DIR}"
popd
# copy tar files to the same directory as script files
cp "${RUNTIME_DIR}/CMSRunAnalysis.tar.gz" \
   "${RUNTIME_DIR}/TaskManagerRun.tar.gz" \
   "${INSTALL_DIR}/data"

# remove unuse lib dir from `install_system`
rm -rf "${INSTALL_DIR:?}/lib"
