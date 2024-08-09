#!/bin/sh
source /etc/profile
echo "Beginning dag_boostrap_startup.sh at $(date)"
echo "On host: $(hostname)"
echo "At path: $(pwd)"
echo "As user: $(whoami)"
set -x

# Touch a bunch of files to make sure job doesn't go on hold for missing files
for FILE in $1.dagman.out RunJobs.dag.dagman.out dbs_discovery.err dbs_discovery.out job_splitting.err job_splitting.out; do
    touch $FILE
done

export PATH="/usr/local/bin:/bin:/usr/bin:/usr/bin:$PATH"
export LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH

OS_Version=`cat /etc/os-release |grep VERSION_ID|cut -d= -f2|tr -d \"|cut -d. -f1`

if [ "$OS_Version" = "7" ]
then
  # following two lines are needed to use pycurl on python3 without the full COMP or CMSSW env.
  export PYTHONPATH=$PYTHONPATH:/data/srv/pycurl3/7.44.1
  source /cvmfs/cms.cern.ch/slc7_amd64_gcc900/external/curl/7.59.0/etc/profile.d/init.sh
elif [ "$OS_Version" = "9" ]
then
  # alma9 comes with working curl and pycurl !
  :  # ":" means do nothing in bash, like pass in python
else
  echo " WE DO NOT SUPPORT OS_Version = $OS_Version "
  exit 1
fi

srcname=$0
env > ${srcname%.sh}.env

if [ "X$useHtcV2" != "X" ]; then
  echo "WILL USE HTC BINDINGS V2"  # make a note in logfile dag_bootstrap.out
  touch USE_HTC_V2_BINDINGS        # create an empty file to make it easy to tell when investigating
fi

#Sourcing Remote Condor setup
source_script=`grep '^RemoteCondorSetup =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
if [ "X$source_script" != "X" ] && [ -e $source_script ]; then
    source $source_script
fi

mkdir -p retry_info
mkdir -p resubmit_info
mkdir -p defer_info
mkdir -p transfer_info


#This is the only file transfered from the TW to the schedd. Can be downloaded for the "preparelocal" client command
TARBALL_NAME="InputFiles.tar.gz"
tar --keep-newer-files -mxvf $TARBALL_NAME
if [[ $? != 0 ]]; then
    echo "Error: Unable to unpack the Input files of the task." >&2
    condor_qedit $CONDOR_ID DagmanHoldReason "'Unable to unpack the input files of the task.'"
    exit 1
fi


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

# MAX_IDLE and MAX_POST are set in TaskWorker configuration and ServerUtilities.py.
MAX_IDLE=1000
MAX_IDLE_TMP=`grep '^CRAB_MaxIdle =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
if [ "X$MAX_IDLE_TMP" != "X" ]; then
    MAX_IDLE=$MAX_IDLE_TMP
fi

MAX_POST=20
MAX_POST_TMP=`grep '^CRAB_MaxPost =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
if [ "X$MAX_POST_TMP" != "X" ]; then
    MAX_POST=$MAX_POST_TMP
fi

# Bootstrap the runtime - we want to do this before DAG is submitted
# so all the children don't try this at once.
if [ "X$TASKWORKER_ENV" = "X" -a ! -e CRAB3.zip ]; then
    command -v python3 > /dev/null
    rc=$?
    if [[ $rc != 0 ]]; then
        echo "Error: Python isn't available on `hostname`." >&2
        echo "Error: Bootstrap execution requires python3" >&2
        condor_qedit $CONDOR_ID DagmanHoldReason "'Error: Bootstrap execution requires python3.'"
        exit 1
    else
        echo "I found python3 at.."
        echo `which python3`
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
    tar xfm $TARBALL_NAME
    if [[ $? != 0 ]]; then
        echo "Error: Unable to unpack the task manager runtime environment." >&2
        condor_qedit $CONDOR_ID DagmanHoldReason "'Unable to unpack the task manager runtime environment.'"
        exit 1
    fi
    echo "unzip CRAB3.zip ..."
    unzip -oq CRAB3.zip
    #ls -lagoh

    export TASKWORKER_ENV="1"
fi

# make a local copy of classAds, so that we can re-run this later
# if needed to facilitate debugging
cp $_CONDOR_JOB_AD  ./_CONDOR_JOB_AD

# Recalculate the black / whitelist
if [ -e AdjustSites.py ]; then
    export schedd_name=`condor_config_val schedd_name`
    echo "Execute AdjustSites.py ..."
    python3 AdjustSites.py
    ret=$?
    if [ $ret -eq 1 ]; then
        echo "Error: AdjustSites.py failed to update the webdir." >&2
        condor_qedit $CONDOR_ID DagmanHoldReason "'AdjustSites.py failed to update the webdir.'"
        exit 1
    elif [ $ret -eq 2 ]; then
        echo "Error: Cannot get data from REST Interface" >&2
        condor_qedit $CONDOR_ID DagmanHoldReason "'Cannot get data from REST Interface.'"
        exit 1   
    elif [ $ret -eq 3 ]; then
        echo "Error: this dagman does not match task information in TASKS DB" >&2
        condor_qedit $CONDOR_ID DagmanHoldReason "'This dagman does not match task information in TASKS DB'"
        exit 1
    fi
else
    echo "Error: AdjustSites.py does not exist." >&2
    condor_qedit $CONDOR_ID DagmanHoldReason "'AdjustSites.py does not exist.'"
    exit 1
fi

# Decide if this task will use enable REUSE flag for FTS ASO jobs
# Do it here so that decision stays for task lifetime, even if schedd configuratoion
# is changed at some point
if [ -f /etc/use_fts_reuse ] ;
then
    echo "Found file /etc/use_fts_reuse. Set this task to enable REUSE flag in FTS"
    touch USE_FTS_REUSE
fi

export _CONDOR_DAGMAN_LOG=$PWD/$1.dagman.out
export _CONDOR_DAGMAN_GENERATE_SUBDAG_SUBMITS=False
export _CONDOR_MAX_DAGMAN_LOG=0

# Export path where final job is written. This requires enable the PER_JOB_HISTORY_DIR
# feature in HTCondor configuration. In case this feature is not turn on, CRAB3
# will use old logic to get final job ads.
# New logic: Read job ad file which is written by HTCondor to a dictionary
# Old logic: Use condor_q -debug -userlog <user_log_file>.
# Please remember that any condor_q command is expensive to scheduler.
# See: https://github.com/dmwm/CRABServer/issues/4618
export _CONDOR_PER_JOB_HISTORY_DIR=`condor_config_val PER_JOB_HISTORY_DIR`
mkdir -pv finished_jobs

CONDOR_VERSION=`condor_version | head -n 1`
PROC_ID=`grep '^ProcId =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
CLUSTER_ID=`grep '^ClusterId =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
REQUEST_NAME=`grep '^CRAB_ReqName =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
export CONDOR_ID="${CLUSTER_ID}.${PROC_ID}"
echo "CONDOR_ID set to ${CONDOR_ID}"
#ls -lagoh
if [ "X" == "X$X509_USER_PROXY" ] || [ ! -e $X509_USER_PROXY ]; then
    echo "Failed to find a proxy at: $X509_USER_PROXY"
    condor_qedit $CONDOR_ID DagmanHoldReason "'Failed to find users proxy.'"
    EXIT_STATUS=5
elif [ ! -r $X509_USER_PROXY ]; then
    echo "The proxy is unreadable for some reason"
    condor_qedit $CONDOR_ID DagmanHoldReason "'The proxy is unreadable for some reason'"
    EXIT_STATUS=6
else
    echo "Proxy found at $X509_USER_PROXY"
    # --- New status prototype ---
    # This jdl is created here because we need to identify the task on which the daemon will run,
    # which is done by passing the cluster_id of the dagman to the daemon process via the jdl. With this, we are
    # able to use condor_q in the daemon to check if the task/job status will no longed be updated
    # and if the daemon needs to exit.
    if [ ! -f task_process/task_process_running ];
    then
        echo "creating and executing task process daemon jdl"
        TASKNAME=`grep '^CRAB_ReqName =' $_CONDOR_JOB_AD | awk '{print $NF;}'`
        USERNAME=`grep '^CRAB_UserHN =' $_CONDOR_JOB_AD | awk '{print $NF;}'`
        CMSTYPE=`grep '^CMS_Type =' $_CONDOR_JOB_AD | awk '{print $NF;}'`
        CMSWMTOOL=`grep '^CMS_WMTool =' $_CONDOR_JOB_AD | awk '{print $NF;}'`
        CMSTTASKYPE=`grep '^CMS_TaskType =' $_CONDOR_JOB_AD | awk '{print $NF;}'`
        CMSSUBMISSIONTOOL=`grep '^CMS_SubmissionTool =' $_CONDOR_JOB_AD | awk '{print $NF;}'`
cat > task_process/daemon.jdl << EOF
Universe      = local
Executable    = task_process/task_proc_wrapper.sh
Arguments     = $REQUEST_NAME
Log           = task_process/daemon.PC.log
Output        = task_process/daemon.out.\$(Cluster).\$(Process)
Error         = task_process/daemon.err.\$(Cluster).\$(Process)
+CRAB_ReqName = $TASKNAME
+CRAB_UserHN  = $USERNAME
+CMS_Type     = $CMSTYPE
+CMS_WMTool   = $CMSWMTOOL
+CMS_TaskType = $CMSTTASKYPE
+CMS_SubmissionTool = $CMSSUBMISSIONTOOL

Queue 1
EOF

    if [ "X$useHtcV2" != "X" ]; then
      # add a directitve to the task_process submission JDL to pass this in the env. of the job
      cat >> task_process/daemon.jdl << EOF
environment = "useHtcV2=True"
EOF
    fi
        chmod +x task_process/task_proc_wrapper.sh
        condor_submit task_process/daemon.jdl
    else
        echo "task_process/task_process_running found, not submitting the daemon task"
    fi
    # --- End new status prototype ---

    echo "executing condor_dagman"
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
    #
    # **Warning**
    # This is also done in the PostJob to submit subdags for tail-catching
    # and automated splitting.  Values below will also have to be changed
    # there in (createSubdagSubmission)!
    exec nice -n 19 condor_dagman -f -l . -Lockfile $PWD/$1.lock -DoRecov -AutoRescue 0 -MaxPre 20 -MaxIdle $MAX_IDLE -MaxPost $MAX_POST -Dag $PWD/$1 -Dagman `which condor_dagman` -CsdVersion "$CONDOR_VERSION" -debug 4 -verbose
    EXIT_STATUS=$?
    echo "condor_dagman terminated with EXIT_STATUS=${EXIT_STATUS} at `date`"
fi
exit $EXIT_STATUS
