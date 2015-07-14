#!/bin/sh
source /etc/profile
echo "Beginning dag_boostrap_startup.sh at $(date)"
echo "On host: $(hostname)"
echo "At path: $(pwd)"
echo "As user: $(whoami)"
set -x

# Touch a bunch of files to make sure job doesn't go on hold for missing files
for FILE in $1.dagman.out RunJobs.dag RunJobs.dag.dagman.out dbs_discovery.err dbs_discovery.out job_splitting.err job_splitting.out; do
    touch $FILE
done

# Add in python paths for UCSD and CERN submit hosts.  We'll get RemoteCondorSetup working again in the future.
export PATH="/opt/glidecondor/bin:/opt/glidecondor/sbin:/usr/local/bin:/bin:/usr/bin:/usr/bin:$PATH"
export PATH="/data/srv/glidecondor/bin:/data/srv/glidecondor/sbin:/usr/local/bin:/bin:/usr/bin:/usr/bin:$PATH"
export PYTHONPATH=/opt/glidecondor/lib/python:$PYTHONPATH
export PYTHONPATH=/data/srv/glidecondor/lib/python2.6:$PYTHONPATH
export LD_LIBRARY_PATH=/opt/glidecondor/lib:/opt/glidecondor/lib/condor:.:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/data/srv/glidecondor/lib:/data/srv/glidecondor/lib/condor:.:$LD_LIBRARY_PATH

mkdir -p retry_info
mkdir -p resubmit_info

# Note that the scheduler universe populate the .job.ad from 8.3.2 version.
# Before it was written by this script using condor_q (This command is expensive to Scheduler)
counter=0
while [[ (counter -lt 10) && (!(-e $_CONDOR_JOB_AD) || ($(cat $_CONDOR_JOB_AD | wc -l) -lt 3)) ]]; do
    sleep $((counter*3+1))
    let counter=counter+1
done
# Have a fallback to condor_q in the case of HTCondor bug.
if [ $(cat $_CONDOR_JOB_AD | wc -l) -lt 3 ]; then
    if [ "X$CONDOR_ID" != "X" ]; then
        # TODO: Report that condor_q was executed!
        counter=0
        while [[ (counter -lt 10) && (!(-e $_CONDOR_JOB_AD) || ($(cat $_CONDOR_JOB_AD | wc -l) -lt 3)) ]]; do
            condor_q $CONDOR_ID -l | grep -v '^$' > $_CONDOR_JOB_AD
            sleep $((counter*3+1))
            let counter=counter+1
        done
    else
        echo "Error: CONDOR_ID is unknown."
        echo "Error: Failed to get ClassAds on `hostname`." >&2
        echo "Error: dag_bootstrap_startup requires ClassAds for execution." >&2
        exit 1
    fi
fi

#Sourcing Remote Condor setup
source_script=`grep '^RemoteCondorSetup =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
if [ "X$source_script" != "X" ] && [ -e $source_script ]; then
    source $source_script
fi

#MAX_POST is set in TaskWorker configuration.
MAX_POST=20
MAX_POST_TMP=`grep '^CRAB_MaxPost =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
if [ "X$MAX_POST_TMP" != "X" ]; then
    MAX_POST=$MAX_POST_TMP
fi

# Bootstrap the runtime - we want to do this before DAG is submitted
# so all the children don't try this at once.
if [ "X$TASKWORKER_ENV" = "X" -a ! -e CRAB3.zip ]; then
    command -v python2.6 > /dev/null
    rc=$?
    if [[ $rc != 0 ]]; then
        echo "Error: Python2.6 isn't available on `hostname`." >&2
        echo "Error: Bootstrap execution requires python2.6" >&2
        condor_qedit $CONDOR_ID DagmanHoldReason "'Error: Bootstrap execution requires python2.6.'"
        exit 1
    else
        echo "I found python2.6 at.."
        echo `which python2.6`
    fi

    if [[ "X$CRAB_TASKMANAGER_TARBALL" == "X" ]]; then
        # If CRAB_TASKMANAGER_TARBALL empty you have to
        # specify CRAB_TARBALL_URL with full url to tarball
        CRAB_TASKMANAGER_TARBALL="$CRAB_TARBALL_URL"
    fi

    if [[ "X$CRAB_TASKMANAGER_TARBALL" != "Xlocal" ]]; then
        # pass, we'll just use that value
        echo "Downloading tarball from $CRAB_TASKMANAGER_TARBALL"
        curl $CRAB_TASKMANAGER_TARBALL > TaskManagerRun.tar.gz
        if [[ $? != 0 ]]; then
            echo "Error: Unable to download the task manager runtime environment." >&2
            condor_qedit $CONDOR_ID DagmanHoldReason "'Unable to download the task manager runtime environment.'"
            exit 1
        fi
    else
        echo "Using tarball shipped within condor"
    fi

    TARBALL_NAME=TaskManagerRun.tar.gz
    tar xvfzm $TARBALL_NAME
    if [[ $? != 0 ]]; then
        echo "Error: Unable to unpack the task manager runtime environment." >&2
        condor_qedit $CONDOR_ID DagmanHoldReason "'Unable to unpack the task manager runtime environment.'"
        exit 1
    fi
    unzip CRAB3.zip
    ls -lah

    export TASKWORKER_ENV="1"
fi

# Recalculate the black / whitelist
if [ -e AdjustSites.py ]; then
    python2.6 AdjustSites.py
else
    echo "Error: AdjustSites.py does not exist." >&2
    condor_qedit $CONDOR_ID DagmanHoldReason "'AdjustSites.py does not exist.'"
    exit 1 
fi

# Fix for issues with condor_rm.  See https://htcondor-wiki.cs.wisc.edu/index.cgi/tktview?tn=4615
# This can be removed after all schedds are upgraded to 8.3.2
cat > condor_rm_fix << EOF
#!/bin/sh
# Keep a copy of the original file descriptors open, in case
# if HTCondor is confused when the pipe closes before the 
# executable finishes
exec 3<&1-
exec 4<&2-
# Log the "real output" separately.
exec &>>RunJobs.dag.dagman.out

set -x
condor_qedit "\$@" JobStatusOnRelease 3
exec condor_rm "\$@"
EOF

chmod +x condor_rm_fix

export _condor_DAGMAN_CONDOR_RM_EXE=$PWD/condor_rm_fix

export _CONDOR_DAGMAN_LOG=$PWD/$1.dagman.out
export _CONDOR_MAX_DAGMAN_LOG=0

# Export path where final job is written. This requires enable the PER_JOB_HISTORY_DIR
# feature in HTCondor configuration. In case this feature is not turn on, CRAB3
# will use old logic to get final job ads.
# New logic: Read job ad file which is written by HTCondor to a dictionary
# Old logic: Use condor_q -debug -userlog <user_log_file>.
# Please remember that any condor_q command is expensive to scheduler.
# See: https://github.com/dmwm/CRABServer/issues/4618
export _CONDOR_PER_JOB_HISTORY_DIR=`condor_config_val PER_JOB_HISTORY_DIR`
mkdir -p finished_jobs

CONDOR_VERSION=`condor_version | head -n 1`
PROC_ID=`grep '^ProcId =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
CLUSTER_ID=`grep '^ClusterId =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
export CONDOR_ID="${CLUSTER_ID}.${PROC_ID}"
ls -lah
if [ "X" == "X$X509_USER_PROXY" ] || [ ! -e $X509_USER_PROXY ]; then
    echo "Failed to find a proxy at: $X509_USER_PROXY"
    condor_qedit $CONDOR_ID DagmanHoldReason "'Failed to find users proxy.'"
    EXIT_STATUS=5
elif [ ! -r $X509_USER_PROXY ]; then
    echo "The proxy is unreadable for some reason"
    condor_qedit $CONDOR_ID DagmanHoldReason "'The proxy is unreadable for some reason'"
    EXIT_STATUS=6
else
    # Documentation about condor_dagman: http://research.cs.wisc.edu/htcondor/manual/v8.3/condor_dagman.html
    # In particular:
    # -autorescue 0|1
    #     Whether to automatically run the newest rescue DAG for the given DAG file, if one exists (0 = false, 1 = true).
    # -DoRecovery
    #     Causes condor_dagman to start in recovery mode. This means that it reads the relevant job user log(s) and catches up
    #     to the given DAG's previous state before submitting any new jobs.
    # More about running DAGMan in rescue or recovery mode in the HTCondor manual, section 2.10:
    # http://research.cs.wisc.edu/htcondor/manual/v8.3/2_10DAGMan_Applications.html -
    # see subsections 2.10.9 "The Rescue DAG" and 2.10.10 "DAG Recovery".
    #
    # Re-nice the process so, even when we churn through lots of processes, we never starve the schedd or shadows for cycles.
    exec nice -n 19 condor_dagman -f -l . -Lockfile $PWD/$1.lock -DoRecov -AutoRescue 0 -MaxPre 20 -MaxIdle 1000 -MaxPost $MAX_POST -Dag $PWD/$1 -Dagman `which condor_dagman` -CsdVersion "$CONDOR_VERSION" -debug 4 -verbose
    EXIT_STATUS=$?
fi
exit $EXIT_STATUS
