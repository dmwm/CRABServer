#!/bin/bash

function cache_status {
    python task_process/cache_status.py
}

function check_exit {
    ARG_DAG_STATUS=$1
    ARG_ENTERED_CUR_STATUS=$2
    # Checks if the TP can exit without losing any possible future updates to the status of the task
    # First checks that the DAG is in a final state
    # If that passes, checks if the dag has been in a final state for long enough (currently 24h)
    # It will echo 1 if TP can exit, otherwise 0

    if [[ "$ARG_DAG_STATUS" != "1" && "$ARG_DAG_STATUS" != "2" && "$ARG_DAG_STATUS" != "init" && "$ARG_DAG_STATUS" ]]; then
        # If the dag is in a final state, check the age of ENTERED_CUR_STATUS.
        echo $(( ( $(date +"%s") - $ARG_ENTERED_CUR_STATUS ) >  24 * 3600 ))
        return
    fi
    # Cannot exit yet
    echo 0
}

function perform_condorq {
    read DAG_STATUS ENTERED_CUR_STATUS <<< `condor_q $CLUSTER_ID -af JobStatus EnteredCurrentStatus`
    TIME_OF_LAST_QUERY=$(date +"%s")
    echo "Query done on $(date '+%Y/%m/%d %H:%M:%S %Z')"
}

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
    # This is part of the logic for handling empty condor_q results. Because condor_q can sometimes return empty,
    # and this script needs to know if the dag is in a final state before exiting, we ignore empty condor_q results
    # and continue looping. However, because this could also mean that the dag has been removed from the queue,
    # we check for the existence of the caching script. This file, along with everything else in the spool directory,
    # is removed almost immediately by htcondor after the dag disappears from the queue. Otherwise, this wrapper script
    # which is loaded in memory, would keep trying to execute the non-existent file.
    if [[ ! -f task_process/cache_status.py  ]]; then
        echo "task_process/cache_status.py file not found, exiting."
        exit 1
    fi

    # Run the parsing script
    cache_status
    sleep 300s

    # Calculate how much time has passed since the last condor_q and perform it again if it has been long enough.
    if [ $(($(date +"%s") - $TIME_OF_LAST_QUERY)) -gt $(($HOURS_BETWEEN_QUERIES * 3600)) ]; then
        perform_condorq
    fi

    # Once the dag's status changes from 1 (idle) or 2 (running) to something else,
    # (normally) no further updates are expected to the log files and the task_process can exit.
    # However, in the case that a crab kill command is issued, Task Worker performs two operations:
    # 1) Hold the DAG with condor_hold,
    # 2) Remove all of the jobs with condor_rm.
    # If a task is large, it will take some time for all of the jobs to be removed. If during this time
    # we run condor_q, see that the DAG is held and decide to exit immediately, some jobs may still be
    # in the process of getting removed and their status may not be updated before the wrapper exits. To get around this,
    # we won't exit until the dag has spent at least 24 hours in it's latest state (EnteredCurrentStatus classad).
    # This should give enough time for any changes to be propagated to the log files that we parse in the caching script.
    # This method is better than a simple sleep because of the possibility of resubmission.
    #
    # Note that here we also ignore an empty DAG_STATUS result because it could be a temporary problem with condor_q
    # simply returning empty. If the dag isn't actually in the queue anymore, the check for an existing caching script
    # at the start of the loop should catch that and exit.
    echo "Dag status code: $DAG_STATUS Entered current status date: $(date -d @$ENTERED_CUR_STATUS '+%Y/%m/%d %H:%M:%S %Z')"
    if [ "$(check_exit $DAG_STATUS $ENTERED_CUR_STATUS)" == "1" ]; then
        # Before we really decide to exit, we should run condor_q once more 
        # to get the latest info about DAG_STATUS and ENTERED_CUR_STATUS. Even though check_exit may pass successfully,
        # because we do condor_q only every 24h (and also wait for 24 hours after ENTERED_CUR_STATUS),
        # the information used in check_exit could be out of date - the task may have been resubmitted 
        # after our last condor_q, for example.
        echo "Running an extra condor_q check before exitting"
        perform_condorq
        if [ "$(check_exit $DAG_STATUS $ENTERED_CUR_STATUS)" == "1" ]; then

            echo "Dag has been in one of the final states for over 24 hours."
            echo "Caching the status one last time, removing the task_process/task_process_running file and exiting."

            cache_status
            rm task_process/task_process_running

            exit 0
        fi
        echo "Cannot exit yet! DAG_STATUS: $DAG_STATUS ENTERED_CUR_STATUS: $ENTERED_CUR_STATUS"
    fi
done
