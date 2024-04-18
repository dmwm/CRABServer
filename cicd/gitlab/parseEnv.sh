#! /bin/bash
set -euo pipefail

# assume $CI_COMMIT_TAG is `pypi-`
TAG=$1
VALIDATE_TAG='^pypi-(devthree|preprod).*'
if [[ ! $TAG =~ $VALIDATE_TAG ]]; then
    >&2 echo "fail to parse env from string: $TAG"
    exit 1
fi
IFS='-' read -ra TMPSTR <<< "${TAG}"
DEV_ENV=${TMPSTR[1]}

echo -n > .env
if [[ ${DEV_ENV} == 'devthree' ]]; then
    {
        echo "KUBECONTEXT=cmsweb-test12"
        echo "Environment=crab-dev-tw03"
        echo "REST_Instance=test12"
    } >> .env
fi
