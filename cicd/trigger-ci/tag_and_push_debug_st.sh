#! /bin/bash
set -euo pipefail

ENV=test2
# if ./env  file exist, source it
[[ -f ./env ]] && source ./env
echo ${ENV}
TAG=pypi-${ENV}-$(date +"%s")
git tag $TAG
git push gitlab $TAG -o ci.variable="MANUAL_CI_PIPELINE_ID=7990590"
