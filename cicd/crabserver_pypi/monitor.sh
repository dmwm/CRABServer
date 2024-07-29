#!/bin/bash

# inject some delays to wait crabrest create pid file
sleep 3

# Check if pid exist before start process_monitor.sh scripts
# process_monitor.sh need PID to feed to process_exporter
RETRY=5
for ((i=1; i<=${RETRY}; i++))
do
    if [ -f /data/srv/state/crabserver/crabserver.pid ]; then
        nohup process_monitor.sh /data/srv/state/crabserver/crabserver.pid crabserver ":18270" 15 2>&1 1>& crabserver_monitor.log < /dev/null &
        break
    elif [ -f /data/srv/state/crabserver/pid ]; then
        nohup process_monitor.sh /data/srv/state/crabserver/pid crabserver ":18270" 15 2>&1 1>& crabserver_monitor.log < /dev/null &
        break
    else
        if [ ${i} -eq ${RETRY} ]; then
            echo "Cannot find crabserver's PID file"
        else
            sleep 1
        fi
    fi
done
