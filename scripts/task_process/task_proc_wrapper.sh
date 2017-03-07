#!/bin/bash
if [ ! -f /etc/enable_task_daemon ]; then
    echo "/etc/enable_task_daemon file not found, not starting the task daemon and exiting"
    exit 1
fi

touch task_process/task_process_running
echo "Starting a new task_process, creating task_process_running file"


HOURS_BETWEEN_QUERIES=24

# Cluster ID is passed from the dagman_bootstrap_startup.sh and points to the main dag
CLUSTER_ID=$1
echo "CLUSTER_ID: $CLUSTER_ID"

# Sleeping until files in the spool dir are created. TODO - make this smarter
sleep 60s
TIME_OF_LAST_QUERY=$(date +"%s")

# Loop will exit when condor_q does not return 1 (idle) or 2 (running), meaning that the dag reached a final state
# and no more changes should happen to the status.
# For the initial query period, set DAG_STATUS to something other than empty. Since querying condor immediatly after
# submission is most likely pointless and relatively expensive, the script will run normally and perform the query later.
DAG_STATUS="init"

echo "Starting task daemon wrapper"
while true
do
    # Run the parsing script
    python task_process/cache_status.py
    sleep 300s

    # Calculate how much time has passed since the last condor_q and perform it again if it has been long enough.
    if [ $(($(date +"%s") - $TIME_OF_LAST_QUERY)) -gt $(($HOURS_BETWEEN_QUERIES * 3600)) ]; then
        DAG_STATUS=$(condor_q $CLUSTER_ID -af JobStatus)
        TIME_OF_LAST_QUERY=$(date +"%s")

        echo "Query done on $(date)"
    fi

    # Once the dag's status changes from 1 (idle) or 2 (running) to something else,
    # no further updates are expected to the log files and the task_process can exit.
    # In a rare case (if this script does condor_q right after the the dag changes states),
    # it is actually possible that task_process would exit before the changes contributing to the status are propagated.
    # Adding a short sleep would solve this, however, that then presents another problem of task_process not starting
    # at all if the task is resubmitted during this short sleep period.
    echo "Dag status code: $DAG_STATUS"
    if [[ "$DAG_STATUS" != 1 && "$DAG_STATUS" != 2 && "$DAG_STATUS" != "init" ]]; then
        echo "Dag is in one of the final states. Removing the task_process/task_process_running file and exiting."
        rm task_process/task_process_running

        exit 0
    fi
done
