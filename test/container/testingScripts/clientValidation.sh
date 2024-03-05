#!/bin/bash

source setupCRABClient.sh

{
 
  TMP_BUFFER=$(mktemp -dt )/client_validation.log
  STORAGE_SITE="T2_CH_CERN"
  PROXY=$(voms-proxy-info -path 2>&1)
  OUTPUTDIR="$PWD/logdir"
  TASK_TO_SUBMIT="crabConfiguration.py"
  TASK_DIR="${WORK_DIR}/CRABServer/test/clientValidationTasks"

  #list of commands to execute for full testing (sl7/8)
  FULL_TEST=(createmyproxy checkusername checkwrite tasks preparelocal status report getlog getoutput recover)
  #list of commands to execute on sl6
  SL6_TESTS=(status checkusername)
  
  #${TEST_LIST} comes from Jenkins and is used to specify which testing should be done: PR_TEST or FULL_TEST
  TEST_TO_EXECUTE=${TEST_LIST}[@]

  function logMsg() {
    local kind=$1
    local msg=$2
    case $kind in
    OK)
      if [ "$LSB_DISTRO" == "true" ]; then
        log_success_msg "$msg"
      else
        printf "$kind %-80s"
        #echo_success
      fi
      ;;
    WARNING)
      if [ "$LSB_DISTRO" == "true" ]; then
        log_warning_msg "$msg"
      else
        printf "$kind %-80s"
        #echo_warning
      fi
      ;;
    FAILED)
      if [ "$LSB_DISTRO" == "true" ]; then
        log_failure_msg "$msg"
      else
        printf "$kind %-80s"
        #echo_failure
      fi
      ;;
    esac
  }

  function checkThisCommand() {
    local cmd="$1"
    local parms="$2"

    if [[ ! " ${!TEST_TO_EXECUTE} " =~ " ${cmd} " ]]; then
         :
    else
      echo "____________"
    	echo -ne "TEST_COMMAND: crab $cmd $parms \n" 
    	crab $cmd $parms 2>&1 > $TMP_BUFFER
    	if [ $? != 0 ]; then
      		error=`cat $TMP_BUFFER`
      		if [[ $error == *"Cannot retrieve the status_cache file"* ]]; then
        		echo "TEST_RESULT: `logMsg WARNING`"
      		else
        		echo "TEST_RESULT: `logMsg FAILED`"
      		fi
    	else
          
      		echo "TEST_RESULT: `logMsg OK`"
    	fi
    	echo "TEST_MESSAGE:"
    	cat $TMP_BUFFER
    	echo -e "____________\n"
     fi
  }

  # check for a valid proxy
  function checkProxy(){
    voms-proxy-init -rfc -voms cms -valid 192:00 > /dev/null 2>&1
    export PROXY=$(voms-proxy-info -path 2>&1)

    noProxy=`echo "$PROXY" | grep 'Proxy not found'`
    if [ "$noProxy" != "" ];then
      echo -ne "Fatal Proxy error: No proxy found..Please create one to proceed with the validation\n"
      exit 1
    fi

    voms-proxy-info -all
    export X509_USER_PROXY=$PROXY

  }

  TMP_PARM1=("")
  function checkCmdParam() {
    cmdArgs=($(crab "$1" -h | sed -n '/--help/,$p' | grep '^  -' | awk '{print $1}' | xargs | sed 's/-h,//g'))
    TMP_PARM1="${cmdArgs[@]}"
  }

  USETHISPARMS=()
  INITPARMS=()
  function feedParms() {
    local parms=($INITPARMS)
    local values=($1)
    parmsToUse=""
    local idx=0
    for p in "${parms[@]}"; do
      vtp=''
      if [[ "$p" == *'|'* ]]; then
        vtp=$(echo $p | cut -d'|' -f${values[$idx]} | sed "s|'||g")
      else
        vtp=$(echo "$p=${values[$idx]} ")
      fi
      idx=$((idx + 1))
      parmsToUse="$parmsToUse $vtp"
    done
    #echo $parmsToUse
    USETHISPARMS+=("$parmsToUse")
  }

  checkProxy

  ##################################################
  # START CRABCLIENT VALIDATION
  ##################################################

  ### 1. test crab createmyproxy -h, --days=100
  USETHISPARMS=()
  INITPARMS="--days"
  feedParms "100"
  for parm in "${USETHISPARMS[@]}"; do
      checkThisCommand createmyproxy "$parm"
  done


  ### 2. test crab checkusername -h, --proxy=PROXY
  USETHISPARMS=()
  INITPARMS="--proxy"
  feedParms "$PROXY"
  checkThisCommand checkusername "${USETHISPARMS[@]}"


  ### 3. crab checkwrite --site=SITENAME --proxy=PROXY --checksum=CHECKSUM
  USETHISPARMS=()
  INITPARMS="--site --proxy"
  feedParms "$STORAGE_SITE $PROXY"
  INITPARMS="--site --proxy --checksum"
  feedParms "$STORAGE_SITE $PROXY yes"
  for parm in "${USETHISPARMS[@]}"; do
      checkThisCommand checkwrite "$parm"
  done


  ### 4. test crab tasks --days=3 --status=PARAMS --proxy=PROXY
  USETHISPARMS=()
  INITPARMS="--days --status --proxy --instance"
  PARAMS=(NEW HOLDING QUEUED SUBMITTED SUBMITFAILED KILLED KILLFAILED RESUBMITFAILED FAILED)
  for st in "${PARAMS[@]}"; do
    feedParms "1 $st $PROXY $REST_Instance"
  done
  for parm in "${USETHISPARMS[@]}"; do
    checkThisCommand tasks "$parm"
  done


  ##################################################
  # START CHECKING SUBMITTED TASK'S STATUS
  ##################################################

  TASKTOTRACK=`cat ${WORK_DIR}/artifacts/submitted_tasks`
  PROJDIR=`crab remake --task=$TASKTOTRACK --instance=$REST_Instance --proxy=$PROXY| grep 'Finished remaking project directory' | awk '{print $6}'`

  ### 5. test crab preparelocal --proxy=PROXY --dir=PROJDIR
  USETHISPARMS=()
  INITPARMS="--proxy --dir"
  feedParms "$PROXY $PROJDIR"
  for param in "${USETHISPARMS[@]}"; do
    checkThisCommand preparelocal "$param"
  done


  ### 6. test crab status --proxy=PROXY --dir=PROJDIR --long --verboseErrors  --sort=SORTING
  USETHISPARMS=()
  INITPARMS="'--long|--verboseErrors|' --proxy --dir"
  for opt in 1 2 3 4; do
    feedParms "$opt $PROXY ${PROJDIR}"
  done
  INITPARMS="--sort  --proxy --dir"
  SORTING=('state' 'site' 'runtime' 'memory' 'cpu' 'retries' 'waste' 'exitcode')
  for st in "${SORTING[@]}"; do
    feedParms "$st $PROXY ${PROJDIR}"
  done
  for param in "${USETHISPARMS[@]}"; do
    checkThisCommand status "$param"
  done

  ### 6b. test  crab status --proxy=PROXY --task=TASKNAME --instance=INSTANCE
  USETHISPARMS=()
  INITPARMS="--proxy --task --instance"
  feedParms "$PROXY $TASKTOTRACK $REST_Instance"
  for param in "${USETHISPARMS[@]}"; do
    checkThisCommand status "$param"
  done

  ### 7. test crab report --proxy=PROXY --dir=PROJDIR --outputdir=OUTPUTDIR
  USETHISPARMS=()
  INITPARMS="--outputdir --proxy --dir"
  feedParms "$OUTPUTDIR $PROXY $PROJDIR"
  for param in "${USETHISPARMS[@]}"; do
    checkThisCommand report "$param"
  done


  ### 8. test crab getlog --quantity=QUANTITY  --short --outputpath=URL --dump --xrootd
  # --jobids=JOBIDS --checksum=CHECKSUM --proxy=PROXY --dir=PROJDIR
  USETHISPARMS=()
  INITPARMS="--quantity '--short|' --outputpath '|--dump|--xrootd' --jobids --checksum  --proxy --dir"
  feedParms "2 1 $OUTPUTDIR 2 2,3 yes $PROXY $PROJDIR"
  feedParms "2 2 $OUTPUTDIR 2 2,3 no  $PROXY $PROJDIR"
  for param in "${USETHISPARMS[@]}";do
    checkThisCommand getlog "$param"
  done


  ### 9. test crab getoutput --quantity=QUANTITY --parallel=NPARALLEL --wait=WAITTIME --outputpath=URL
  # --dump --xrootd --jobids=JOBIDS --checksum=CHECKSUM --proxy=PROXY --dir=PROJDIR
  USETHISPARMS=()
  # use --jobids instead of --quantity
  INITPARMS="--parallel --wait --outputpath '|--dump|--xrootd' --jobids --checksum --proxy --dir"
  feedParms "10 4 $OUTPUTDIR 1 2,3 yes $PROXY $PROJDIR"
  feedParms "10 4 $OUTPUTDIR 1 2,3 no  $PROXY $PROJDIR"
  feedParms "10 4 $OUTPUTDIR 2 2,3 yes $PROXY $PROJDIR"
  feedParms "10 4 $OUTPUTDIR 2 2,3 no  $PROXY $PROJDIR"
  feedParms "10 4 $OUTPUTDIR 3 2,3 yes $PROXY $PROJDIR"
  feedParms "10 4 $OUTPUTDIR 3 2,3 no  $PROXY $PROJDIR"
  # use --quantity instead of jobis
  INITPARMS="--quantity --parallel --wait --outputpath '|--dump|--xrootd' --checksum --proxy --dir"
  feedParms "1 10 4 $OUTPUTDIR 1 yes $PROXY $PROJDIR"
  feedParms "3 10 4 $OUTPUTDIR 1 no  $PROXY $PROJDIR"
  for param in "${USETHISPARMS[@]}";do
    checkThisCommand getoutput "$param"
  done

  ### 10. test crab kill --proxy=PROXY --dir=PROJDIR
  USETHISPARMS=()
  INITPARMS="--proxy --dir"
  feedParms "$PROXY $PROJDIR"
  for param in "${USETHISPARMS[@]}"; do
    checkThisCommand kill "$param"
  done

  ### 11. test crab recover --proxy=PROXY --dir=PROJDIR
  USETHISPARMS=()
  INITPARMS="--proxy --dir"
  feedParms "$PROXY $PROJDIR"
  for param in "${USETHISPARMS[@]}"; do
    checkThisCommand recover "$param"
  done

  ### 12. test  crab recover --proxy=PROXY --task=TASKNAME --instance=INSTANCE
  USETHISPARMS=()
  INITPARMS="--proxy --task --instance"
  feedParms "$PROXY $TASKTOTRACK $REST_Instance"
  for param in "${USETHISPARMS[@]}"; do
    checkThisCommand recover "$param"
  done

} 2>&1 | tee ${WORK_DIR}/client-validation.log
