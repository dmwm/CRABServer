#!/bin/bash
if [ ! -f /etc/enable_status_daemon ]; then
    echo "/etc/enable_status_daemon file not found, not starting the status daemon and exiting"
    exit 1
fi

TIME_BETWEEN_QUERIES=24 #hours
# SECONDS variable is always being automatically incremented.
# for the beginning of this script, set it to a big value so that condor_q can be done initially
SECONDS=$(($TIME_BETWEEN_QUERIES * 3601))

echo "Starting status daemon wrapper"
# Cluster ID is passed from the dagman_bootstrap_startup.sh and points to the main dag
CLUSTER_ID=$1
echo "CLUSTER_ID: $CLUSTER_ID"
# Sleeping until files in the spool dir are created. TODO - make this smarter
sleep 60s
# Loop will exit when the dag code becomes 5 or 6.
while true
do
    
    if [ $SECONDS -gt $(($TIME_BETWEEN_QUERIES * 3600)) ]; then
        echo "$SECONDS since last query, requerying."
        CONDOR_RES=$(condor_q $CLUSTER_ID -af JobStatus)
        LAST_QUERY_DONE=$(date +"%T")
        SECONDS=0
    fi

    echo "Dag status code: $CONDOR_RES"
    if [ ! -f /etc/enable_status_daemon ]; then
        echo "/etc/enable_status_daemon file not found, likely removed after the daemon started. exiting"
        exit 1
    elif [ -z "$CONDOR_RES" ]; then
        echo "dag status code empty. status daemon exiting"
        exit 0
    fi
    python task_process/cache_status.py
    sleep 300s
done
