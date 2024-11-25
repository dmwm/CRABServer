#! /bin/bash
set -euo pipefail
# This is only for test.
# But you can use to release a new production tag by
#   1. Delete all tags with `v3.24XXXX-stable` in registry.
#   2. Run this script with test tag.
#   3. Then, retag individually image by hand.
TAG=v3.240000.test-$(date +"%s")
git tag $TAG
#git push gitlab $TAG
git push gitlab $TAG -o ci.variable="ONLY_BUILD_RELEASE=true"
