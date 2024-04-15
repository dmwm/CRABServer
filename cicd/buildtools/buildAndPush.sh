#! /bin/bash

set -euo pipefail
# make sure we build from Dockerfile directory
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pushd "${SCRIPT_DIR}"

# also tag today, as backup
# assign today something like 240101
today=$(date '+%g%m%d')
docker build -t registry.cern.ch/cmscrab/buildtools:latast -f Dockerfile .
docker tag registry.cern.ch/cmscrab/buildtools:latast registry.cern.ch/cmscrab/buildtools:"${today}"
docker push registry.cern.ch/cmscrab/buildtools:latest
docker push registry.cern.ch/cmscrab/buildtools:"${today}"

# always pop after push
popd
