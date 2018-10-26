#!/bin/bash

function log {
    echo "[$(date +"%F %R")]" $*
}

function cache_status {
    log "Running cache_status.py"
    python task_process/cache_status.py
}

function manage_transfers {
    log "Running transfers.py"
    python task_process/transfers.py 
}

function exit_now {
    # Checks if the TP can exit without losing any possible future updates to the status of the task
    # Returns a string "True" or "False"
    # First checks that the DAG is in a final state
    # If that passes, checks if the dag has been in a final state for long enough (currently 24h)
    # It will echo 1 if TP can exit, otherwise 0

    # TP will only exit if the dag is not in a temporary state ( not
    # idle(1) and not running(2) ), if at least one condor_q was performed
    # (DAG_INFO is not "init") and if the last perfomed condor_q was
    # successfull (DAG_INFO is not empty)
    if [[ "$DAG_INFO" == "init" || ! "$DAG_INFO" ]]; then
        EXIT_NOW=False
        echo $EXIT_NOW
        return
    fi

    # Make sure that all DAGs have exited; Count the ones that are still
    # running or are in the current state for less than 24 hours.
    EXIT_NOW=True
    ONE_DAY=$(( 24 * 3600 ))
    while read CLUSTER_ID DAG_STATUS ENTERED_CUR_STATUS; do
        TIME_IN_THIS_STATUS=$(( ($(date +"%s") - $ENTERED_CUR_STATUS) ))
        if [[ "$DAG_STATUS" == "1" || "$DAG_STATUS" == "2" || $TIME_IN_THIS_STATUS -lt $ONE_DAY ]]; then
            EXIT_NOW=False
        fi
    done <<< "$DAG_INFO"
    echo $EXIT_NOW
}

function dag_status {
    [[ "$DAG_INFO" == "init" || ! "$DAG_INFO" ]] && return
    while read CLUSTER_ID DAG_STATUS ENTERED_CUR_STATUS; do
        log "Dag status code for $CLUSTER_ID: $DAG_STATUS Entered current status date: $(date -d @$ENTERED_CUR_STATUS '+%Y/%m/%d %H:%M:%S %Z')"
    done <<< "$DAG_INFO"
}

function perform_condorq {
    DAG_INFO=$(condor_q -constr 'CRAB_ReqName =?= "'$REQUEST_NAME'" && stringListMember(TaskType, "ROOT PROCESSING TAIL", " ")' -af ClusterId JobStatus EnteredCurrentStatus)
    TIME_OF_LAST_QUERY=$(date +"%s")
    log "HTCondor query of DAG status done on $(date '+%Y/%m/%d %H:%M:%S %Z')"
}

if [ ! -f /etc/enable_task_daemon ]; then
    log "/etc/enable_task_daemon file not found, not starting the task daemon and exiting"
    exit 1
fi

touch task_process/task_process_running
log "Starting a new task_process, creating task_process_running file"


HOURS_BETWEEN_QUERIES=24

# The request name is passed from the dagman_bootstrap_startup.sh and points to the main dag
REQUEST_NAME=$1
log "REQUEST_NAME: $REQUEST_NAME"

# Sleeping until files in the spool dir are created. TODO - make this smarter
sleep 60s
TIME_OF_LAST_QUERY=$(date +"%s")

# Loop will exit when condor_q does not return 1 (idle) or 2 (running), meaning that the dag reached a final state
# and no more changes should happen to the status.
# For the initial query period, set DAG_INFO to something other than empty. Since querying condor immediatly after
# submission is most likely pointless and relatively expensive, the script will run normally and perform the query later.
DAG_INFO="init"

export PYTHONPATH=`pwd`/CRAB3.zip:`pwd`/WMCore.zip:$PYTHONPATH

log "Starting task daemon wrapper"
while true
do
    # This is part of the logic for handling empty condor_q results. Because condor_q can sometimes return empty,
    # and this script needs to know if the dag is in a final state before exiting, we ignore empty condor_q results
    # and continue looping. However, because this could also mean that the dag has been removed from the queue,
    # we check for the existence of the caching script. This file, along with everything else in the spool directory,
    # is removed almost immediately by htcondor after the dag disappears from the queue. Otherwise, this wrapper script
    # which is loaded in memory, would keep trying to execute the non-existent file.
    if [[ ! -f task_process/cache_status.py  ]]; then
        log "task_process/cache_status.py file not found, exiting."
        exit 1
    fi

    # Run the parsing script
    cache_status
    manage_transfers
    sleep 300s

    # Calculate how much time has passed since the last condor_q and perform it again if it has been long enough.
    if [ $(($(date +"%s") - $TIME_OF_LAST_QUERY)) -gt $(($HOURS_BETWEEN_QUERIES * 3600)) ]; then
        perform_condorq
        dag_status
    fi

    # Once the dags' status changes from 1 (idle) or 2 (running) to something else,
    # (normally) no further updates are expected to the log files and the task_process can exit.
    # However, in the case that a crab kill command is issued, Task Worker performs two operations:
    # 1) Hold the DAG with condor_hold,
    # 2) Remove all of the jobs with condor_rm.
    # If a task is large, it will take some time for all of the jobs to be removed. If during this time
    # we run condor_q, see that the DAGs are held and decide to exit immediately, some jobs may still be
    # in the process of getting removed and their status may not be updated before the wrapper exits. To get around this,
    # we won't exit until every dag has spent at least 24 hours in it's latest state (EnteredCurrentStatus classad).
    # This should give enough time for any changes to be propagated to the log files that we parse in the caching script.
    # This method is better than a simple sleep because of the possibility of resubmission.
    #
    # Note that here we also ignore an empty DAG_INFO result because it could be a temporary problem with condor_q
    # simply returning empty. If the dag isn't actually in the queue anymore, the check for an existing caching script
    # at the start of the loop should catch that and exit.

    # Test if 24 hours have elapsed since the last saved condor status
    if [ "$(exit_now)" == "True" ]; then
        # Before we really decide to exit, we should run condor_q once more
        # to get the latest info about the DAGs. Even though exit_now may say yes,
        # because we do condor_q only every 24h (and also wait for 24 hours after ENTERED_CUR_STATUS),
        # the information used in exit_now could be out of date - the task may have been resubmitted
        # after our last condor_q, for example.
        log "Running an extra condor_q check before exitting"
        perform_condorq
        dag_status
        if [ "$(exit_now)" == "True" ]; then
            log "Dag(s) has (have) been in one of the final states for over 24 hours."
            log "Caching the status one last time, removing the task_process/task_process_running file and exiting."

            cache_status
            rm task_process/task_process_running

            exit 0
        fi
        log "Cannot exit yet!"
    fi
done
