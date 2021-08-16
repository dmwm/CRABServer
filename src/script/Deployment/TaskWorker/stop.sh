#!/bin/bash
#  script is used to gracefully stop TW.
#  TW is given checkTimes*timeout seconds to stop, if it is still running after
#  this period, TW and all its slaves are killed by sending SIGKILL signal.

checkTimes=12
timeout=15 #that will give 12*15=180 seconds (3min) for the TW to finish work

TaskMasterPid=$(ps exfww | grep MasterWorker | grep -v grep | head -1 | awk '{print $1}')
kill $TaskMasterPid || exit 0
echo "SIGTERM sent to MasterWorker pid $TaskMasterPid"

for (( i=0; i<$checkTimes; ++i)); do
  # get only alive slaves, i.e. exclude defunct processes
  aliveSlaves=`ps --ppid $TaskMasterPid  -o pid,s | grep -v "Z" | awk '{print $1}' | tail -n +2 | tr '\n' ' '`

  if [ -n "$aliveSlaves" ]; then
    echo "slave(s) PID [ ($aliveSlaves) ] are still running, sleeping for $timeout seconds. ($((i+1)) of $checkTimes try)"
    sleep $timeout
  else
    echo "Slaves gracefully stopped after $((i * timeout)) seconds."
    sleep 1
    break
  fi
done

runningProcesses=`pgrep -f TaskWorker | tr '\n' ' '`
if [ -n "$runningProcesses" ]; then
  echo -e "After max allowed time ($((checkTimes * timeout)) seconds) following TW processes are still running: $runningProcesses \nSending SIGKILL to stop it."
  pkill -9 -f TaskWorker
  echo "Running TW processes: `pgrep -f TaskWorker | tr '\n' ' '`"
else
  echo "TaskWorker master has shutdown successfully."
fi
