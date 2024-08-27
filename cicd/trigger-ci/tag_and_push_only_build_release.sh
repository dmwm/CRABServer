#! /bin/bash
set -euo pipefail

TAG=v3.240000.test-$(date +"%s")
git tag $TAG
#git push gitlab $TAG
git push gitlab $TAG -o ci.variable="ONLY_BUILD_RELEASE=t"
