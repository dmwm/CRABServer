#!/bin/bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
    >&2 echo "Error: position arguments required."
    >&2 echo "Usage: $0 <wmcore_requirements.txt_path> <install_path>"
    exit 1
fi

reqfile="${1}"
installpath="${2}"

if [[ ! -d "${installpath}" ]]; then
    >&2 echo "Error: Install path ${installpath} does not exist or not directory."
    exit 1
fi

wmcore_repo="$(grep -v '^\s*#' "${reqfile}" | cut -d' ' -f1)"
wmcore_version="$(grep -v '^\s*#' "${reqfile}" | cut -d' ' -f2)"
if [[ ${wmcore_repo} =~ /github.com\/dmwm\/WMCore ]]; then
    echo "Installing WMCore ${wmcore_version} from official repository via pip..."
    pip install --no-deps "wmcore==${wmcore_version}"
else
    # simply copy all src/python to install directory
    echo "Installing WMCore ${wmcore_version} from ${wmcore_repo} via clone..."
    git clone  --depth 1 "${wmcore_repo}" -b "${wmcore_version}" WMCore
    cp -rp WMCore/src/python/* "${installpath}"
    cp -rp WMCore/bin/wmc-httpd /usr/local/bin
    rm -rf WMCore
fi
