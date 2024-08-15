#!/bin/bash

set -euo pipefail
set -x

dd if=/dev/urandom of=miniaodfake.root bs=1M count=10

cmsRun -j FrameworkJobReport.xml PSet.py
