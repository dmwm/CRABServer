#!/bin/bash
exec 2>&1


source ./submit_env.sh

# from ./submit_env.sh
setup_cmsset

# from ./submit_env.sh
setup_python_comp

echo "======== Stageout at $(TZ=GMT date) STARTING ========"
# Note we prevent buffering of stdout/err -- this is due to observed issues in mixing of out/err for stageout plugins
PYTHONUNBUFFERED=1 $pythonCommand cmscp.py
