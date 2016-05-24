#!/bin/bash
if [ ! -f /etc/enable_task_daemon ]; then
    echo "/etc/enable_task_daemon file not found, not starting the task daemon and exiting"
    exit 1
fi

HOURS_BETWEEN_QUERIES=24


echo "Starting task daemon wrapper"
# Cluster ID is passed from the dagman_bootstrap_startup.sh and points to the main dag
CLUSTER_ID=$1
echo "CLUSTER_ID: $CLUSTER_ID"
# Sleeping until files in the spool dir are created. TODO - make this smarter
sleep 60s
TIME_OF_LAST_QUERY=$(date +"%s")
# Loop will exit when condor_q returns empty, meaning the main dag has been removed and will no longer be updated.
# For the initial query period, set CONDOR_RES to something other than empty. Since querying condor sooner 
# is most likely pointless, the script will run normally and perform the query later.
CONDOR_RES="init"
while true
do 
    # Calculate how much time has passed since the last query and perform it again if it has been long enough. 
    if [ $(($(date +"%s") - $TIME_OF_LAST_QUERY)) -gt $(($HOURS_BETWEEN_QUERIES * 3600)) ]; then
        CONDOR_RES=$(condor_q $CLUSTER_ID -af JobStatus)
        echo "Query done on $(date)"
        TIME_OF_LAST_QUERY=$(date +"%s")
    fi

    echo "Dag status code: $CONDOR_RES"
    if [ -z "$CONDOR_RES" ]; then
        echo "dag status code empty. task daemon exiting"
        exit 0
    fi
    python task_process/cache_status.py
    sleep 300s
done
