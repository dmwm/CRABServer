#!/bin/bash
# This script is select proper source of WMCore according wmcore_requirements.txt
# which has format:
# <repo> <tag>
# If <repo> is match "github.com/dmwm/WMCore",
# then it will install wmcore from pip via pip.
# Otherwise, clone tag and copy content from WMCore/src/python to install
# directory.
# In case the new tag is not available in PyPI, please clone tag into your
# private repo and change wmcore_requirements.txt point to it.

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

# Note for regex: "Match line start with zero whitespace or more , following by
# '#' character" (the '\s' is the whitespace character).
wmcore_repo="$(grep -v '^\s*#' "${reqfile}" | cut -d' ' -f1)"
wmcore_version="$(grep -v '^\s*#' "${reqfile}" | cut -d' ' -f2)"
if [[ ${wmcore_repo} =~ github.com\/dmwm\/WMCore ]]; then
    echo "Installing WMCore ${wmcore_version} from official repository via pip..."
    pip install --no-deps "wmcore==${wmcore_version}"
else
    # simply copy all src/python to install directory
    echo "Installing WMCore ${wmcore_version} from ${wmcore_repo} via clone..."
    git clone  --depth 1 "${wmcore_repo}" -b "${wmcore_version}" WMCore
    cp -rp WMCore/src/python/* "${installpath}"
    cp -rp WMCore/bin/wmc-httpd /usr/local/bin
    # Patch shebang to python3 (only if it matches the old one)
    sed -i '1 s|^#!/usr/bin/env python$|#!/usr/bin/env python3|' /usr/local/bin/wmc-httpd
    rm -rf WMCore
fi
