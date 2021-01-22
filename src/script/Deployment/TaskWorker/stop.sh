#!/bin/bash

#  script is used to gracefully stop TW.
#  TW is given checkTimes*timeout seconds to stop, if it is still running after
#  this period, TW and all its slaves are killed by sending SIGKILL signal.

checkTimes=6
timeout=30 #that will give 6*30=180 seconds (3min) for the TW to finish work

TaskMasterPid=$(ps exfww | grep MasterWorker | grep -v grep | head -1 | awk '{print $1}')
kill $TaskMasterPid || exit 0

for (( i=0; i<$checkTimes; ++i)); do
  sleep $timeout

  if ps -p $TaskMasterPid >/dev/null; then
    echo "Process $TaskMasterPid is still running."
  else
    echo "Process $TaskMasterPid has shut down successfully"
    exit 0
  fi
done

echo "Could not stop process $TaskMasterPid gracefully. Sending SIGKILL."
pkill -9 -f TaskWorker
