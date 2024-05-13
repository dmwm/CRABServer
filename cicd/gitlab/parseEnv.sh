#! /bin/bash

# Parse deployment env from tag.
# - If match regexp `^pypi-(<env1>|<env2>|...)-.*`, set ENV_NAME to the string
#   in group. Valide env are preprod/test2/test11/test2
#   - Allow override ENV_NAME (from push option or WebUI)
# - If match release tag (e.g., v3.240501), set ENV_NAME to preprod, canoverrid
#   - Allow override RELEASE_NAME (from push option or WebUI)

set -euo pipefail
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

TAG="${1}"

# validate tag
VALIDATE_DEV_TAG='^pypi-(preprod|test2|test11|test12)-.*'
VALIDATE_RELEASE_TAG='^v3\.[0-9]{6}.*'
if [[ $TAG =~ $VALIDATE_DEV_TAG ]]; then # Do not quote regexp variable here
    IFS='-' read -ra TMPSTR <<< "${TAG}"
    ENV_NAME=${ENV_NAME:-${TMPSTR[1]}}
elif [[ $TAG =~ $VALIDATE_RELEASE_TAG ]]; then
    ENV_NAME=${ENV_NAME:-test12}
    RELEASE_NAME="${RELEASE_NAME:-}-stable"
else
    >&2 echo "fail to parse env from string: $TAG"
    exit 1
fi

echo "Use env: ${ENV_NAME}"
echo "Release tag: ${RELEASE_NAME:-}"
cp "${SCRIPT_DIR}/env/${ENV_NAME}" .env
echo "RELEASE_NAME=${RELEASE_NAME:-}" >> .env
echo ".env content:"
cat .env
