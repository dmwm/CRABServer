#!/bin/bash
{ 
# redirect all output to stdout and to log file
# see the last line of the script

# load the logging functions
if [ -f /lib/lsb/init-functions ];then
  . /lib/lsb/init-functions
elif [ -f /etc/init.d/functions ];then
  . /etc/init.d/functions
  LSB_DISTRO="false"
fi

LSB_DISTRO="true"
TMP_BUFFER=$(mktemp -dt "$0.XXXXXXXXXX")/client_validation.log
STORAGE_SITE="T2_CH_CERN"
OLD_CLIENT=/cvmfs/cms.cern.ch/crab3/crab.sh
NEW_CLIENT=/cvmfs/cms.cern.ch/crab3/crab_pre.sh
#NEW_CLIENT=/cvmfs/cms.cern.ch/crab3/crab_pre_light.sh
#NEW_CLIENT='./init-light.sh'
PROXY=`voms-proxy-info -path 2>&1`
OUTPUTDIR="$PWD/logdir"
BOLD=$(tput bold 2>/dev/null)
NORMAL=$(tput sgr0 2>/dev/null)
STOP_IF_FAIL=0

# OCMDS: commands from old client
# NCMDS: commands from the new client

cd CMSSW_7_4_7
eval `scramv1 runtime -sh`

# check if the client is up to date
OV=`source $OLD_CLIENT;crab --version`
NV=`source $NEW_CLIENT;crab --version`

if [ "$OV" == "$NV" ];then
  echo "clients are the same, aborting"
  exit
fi

OCMDS=(`source $OLD_CLIENT;crab --help | sed -n '/Valid commands are/,/To get single command help run/p' | awk '{print $1}'`) # fetch commands from old client
NCMDS=(`source $NEW_CLIENT;crab --help | sed -n '/Valid commands are/,/To get single command help run/p' | awk '{print $1}'`) # fetch commands from new client
OCMDS=(${OCMDS[@]:1:$((${#OCMDS[@]}-2))}) # remove the first and the last element
NCMDS=(${NCMDS[@]:1:$((${#NCMDS[@]}-2))}) # the last number should be 2, I changed it to 3 just to simulate a change in number of commands
# sort all commands
OCMDS=(`echo ${OCMDS[@]} | sort`)
NCMDS=(`echo ${NCMDS[@]} | sort`)

function logMsg(){
  local kind=$1
  local msg=$2
  case $kind in
    success)
      if [ "$LSB_DISTRO"=="true" ];then
        log_success_msg "$msg" 
      else
        printf "$msg %-80s"; echo_success
      fi
      ;;
    warning)
      if [ "$LSB_DISTRO"=="true" ];then
        log_warning_msg "$msg"
      else
        printf "$msg %-80s"; echo_warning
      fi
      ;;
    failure)
      if [ "$LSB_DISTRO"=="true" ];then
        log_failure_msg "$msg"
      else
        printf "$msg %-80s"; echo_failure
      fi
      ;;
  esac
}

# this a debug function that I use to
# execute only some part of the code
# instead traverse all of it.
function jumpTo
{
    label=$1
    cmd=$(sed -n "/$label:/{:a;n;p;ba};" $0 | grep -v ':$')
    eval "$cmd"
    exit
}

# run a crab command and 
# print a msg in case of error
function checkThisCommand(){
  local cmd="$1"
  local parms="$2"
  echo -ne "\n  ${BOLD}Checking crab $cmd${NORMAL}: \n"
  echo -ne "    with options :\n   ${parms}\n"
  crab $cmd $parms 
  # uncomment this for debugging
  #crab $cmd $parms 2>&1 > $TMP_BUFFER
  if [ $? != 0 ];then
    logMsg failure
    cat $TMP_BUFFER
    if [ $STOP_IF_FAIL == 1 ];then
      echo "Stopping... Command crab $cmd needs to work in order to continue the tests"
      exit 2
    fi
  else
    logMsg success
  fi
}

function checkAvailCommands(){
  echo
  testLabel="Checking the number of available commands"

  nocmds=${#OCMDS[@]}
  nncmds=${#NCMDS[@]}

  cmdSetChanged="false"
  msg="   number of commands does not changed"

  if [ "$nocmds" != "$nncmds" ];then
    msg="number of commands changed from previous version"
    cmdSetChanged="true"
  fi

  if [ $cmdSetChanged == "true" ];then
    logMsg warning "$testLabel"
    echo "  -- $msg"
    if [ $nocmds -lt $nncmds ];then
      echo -ne "\tnew commands added...\n"
    else
      echo -ne "\tcommands were removed...\n"
    fi

    echo -ne "\told client has ${nocmds} commands and new client has ${nncmds}\n"
    echo -ne "\t\told command set was: \n\t\t\t${OCMDS[@]}\n"
    echo -ne "\t\tnew command set is: \n\t\t\t${NCMDS[@]}\n"
  else
    logMsg success "$testLabel"
    echo "  -- $msg"
  fi
}

# check for a valid proxy
function checkProxy(){
  noProxy=`echo "$PROXY" | grep 'Proxy not found'`
  if [ "$noProxy" != "" ];then
    echo -ne "No proxy found..\n\tPlease create one to proceed with the validation\n"
    exit
  fi

  # check also if the proxy is still valid
  isValid=`voms-proxy-info -timeleft`
  if [ $isValid == 0 ];then
    echo -ne "Proxy is expired..\n\tPlease create a new one\n"
    exit
  fi

}

TMP_PARM1=("")
function checkCmdParam(){
  cmdArgs=(`crab "$1" -h | sed -n '/--help/,$p' | grep '^  -' | awk '{print $1}' | xargs | sed 's/-h,//g'`)
  TMP_PARM1="${cmdArgs[@]}"
}

USETHISPARMS=()
INITPARMS=()
function feedParms(){
  local parms=($INITPARMS)
  local values=($1)
  parmsToUse=""
  local idx=0
  for p in "${parms[@]}";do
     vtp=''
     if [[ "$p" == *'|'* ]];then  
        vtp=`echo $p | cut -d'|' -f${values[$idx]} | sed "s|'||g"`
     else
        vtp=`echo "$p=${values[$idx]} "`
     fi
     idx=$((idx+1))
     parmsToUse="$parmsToUse $vtp"
  done
  #echo $parmsToUse
  USETHISPARMS+=("$parmsToUse")
}

# load crab environment
source $NEW_CLIENT

# when debugging this script if you want to avoid some test
# just put here a label to jump directly to an especific line 
# like the old "goto" instruction
#
# for instance if you want to start testing crab getoutput 
#
# jumpTo getoutputtest


##################################################
# start crab client validation
#

checkProxy

echo "Using x509 proxy at: " $PROXY
echo "Using $TMP_BUFFER as a temp file"

checkAvailCommands

# from this point we need a project directory to continue
# so first of all let's check if crab status is working
STOP_IF_FAIL=1
checkThisCommand status
STOP_IF_FAIL=0

# this will get project directory for the last task submitted
PROJDIR=`source $NEW_CLIENT;crab status | grep 'CRAB project directory' | awk '{print $4}'`

if [ "X$1" != "X" ];then
  jumpTo $1 
fi

# first check options that does not need a taskname
# checkusername checkwrite task

# crab checkusername  -h, --proxy=PROXY 
USETHISPARMS=()
INITPARMS="--proxy"
feedParms "$PROXY"
checkThisCommand checkusername "${USETHISPARMS[@]}"

# test checkwrite with all parameters
# crab checkwrite --site=SITENAME --lfn=USERLFN --checksum=CHECKSUM --command=COMMAND --proxy=PROXY --voRole=VOROLE --voGroup=VOGROUP
USETHISPARMS=()
INITPARMS="--site --proxy"
feedParms "$STORAGE_SITE $PROXY"

INITPARMS="--site --proxy --checksum"
feedParms "$STORAGE_SITE $PROXY yes"

INITPARMS="--site --proxy --checksum --command"
feedParms "$STORAGE_SITE $PROXY yes LCG"
feedParms "$STORAGE_SITE $PROXY no LCG"
feedParms "$STORAGE_SITE $PROXY yes GFAL"
feedParms "$STORAGE_SITE $PROXY no GFAL"

for parm in "${USETHISPARMS[@]}";do
  checkThisCommand checkwrite "$parm"
done

#tasks:
# crab task --fromdate=YYYY-MM-DD --days=N --status=STATUS --proxy=PROXY --instance=INSTANCE
USETHISPARMS=()
INITPARMS="--days --status --proxy"
PARAMS=(NEW HOLDING QUEUED UPLOADED SUBMITTED SUBMITFAILED KILLED KILLFAILED RESUBMITFAILED FAILED)
# old task status
#PARAMS=(NEW HOLDING QUEUED UPLOADED SUBMITTED SUBMITFAILED KILL KILLED KILLFAILED RESUBMIT RESUBMITFAILED FAILED)
for st in "${PARAMS[@]}";do
  feedParms "1 $st $PROXY"
done

for parm in "${USETHISPARMS[@]}";do
  checkThisCommand tasks "$parm"
done

USETHISPARMS=()
INITPARMS="--fromdate --status --proxy"
DATETOFIND=`date '+%Y-%m-%d' -d '10 days ago'`
for st in "${PARAMS[@]}";do
  feedParms "$DATETOFIND $st $PROXY"
done

for parm in "${USETHISPARMS[@]}";do
  checkThisCommand tasks "$parm"
done

USETHISPARMS=()
INITPARMS="'--json|--long|--idle|--verboseErrors|' --proxy --dir"
for opt in 1 2 3 4;do
   feedParms "$opt $PROXY $PROJDIR"
done

INITPARMS="--sort  --proxy --dir"
SORTING=('state' 'site' 'runtime' 'memory' 'cpu' 'retries' 'waste' 'exitcode')
for st in "${SORTING[@]}";do
  feedParms "$st $PROXY $PROJDIR"
done

for param in "${USETHISPARMS[@]}";do
  checkThisCommand status "$param"
done

#getlogs:
# test crab getlog
# --quantity=QUANTITY --parallel=NPARALLEL --wait=WAITTIME --short --outputpath=URL --dump --xrootd --jobids=JOBIDS --checksum=CHECKSUM 
# --command=COMMAND --proxy=PROXY -d --voRole=VOROLE --voGroup=VOGROUP --instance=INSTANCE
USETHISPARMS=()
INITPARMS="--quantity --parallel --wait '--short|' --outputpath '|--dump|--xrootd' --jobids --checksum --command --proxy --dir"
feedParms "2 10 4 1 $OUTPUTDIR 1 1,2 yes LCG $PROXY $PROJDIR"
feedParms "2 10 4 2 $OUTPUTDIR 1 1,2 no LCG $PROXY $PROJDIR"
feedParms "2 10 4 1 $OUTPUTDIR 2 1,2 yes LCG $PROXY $PROJDIR"
feedParms "2 10 4 2 $OUTPUTDIR 2 1,2 no LCG $PROXY $PROJDIR"
for param in "${USETHISPARMS[@]}";do
  echo "parameters taken: $param"
  checkThisCommand getlog "$param"
done

#getoutput:
# test crab getoutput
# --quantity=QUANTITY --parallel=NPARALLEL --wait=WAITTIME --outputpath=URL --dump --xrootd --jobids=JOBIDS --checksum=CHECKSUM 
# --command=COMMAND --proxy=PROXY -d --voRole=VOROLE --voGroup=VOGROUP --instance=INSTANCE
USETHISPARMS=()
# use --jobids instead of --quantity
INITPARMS="--parallel --wait --outputpath '|--dump|--xrootd' --jobids --checksum --command --dir"
feedParms "10 4 $OUTPUTDIR 1 1,2 yes LCG $PROJDIR"
feedParms "10 4 $OUTPUTDIR 1 1,2 yes GFAL $PROJDIR"
feedParms "10 4 $OUTPUTDIR 1 1,2 no GFAL $PROJDIR"
feedParms "10 4 $OUTPUTDIR 2 1,2 yes LCG $PROJDIR"
feedParms "10 4 $OUTPUTDIR 2 1,2 yes GFAL $PROJDIR"
feedParms "10 4 $OUTPUTDIR 2 1,2 no GFAL $PROJDIR"
feedParms "10 4 $OUTPUTDIR 3 1,2 yes LCG $PROJDIR"
feedParms "10 4 $OUTPUTDIR 3 1,2 yes GFAL $PROJDIR"
feedParms "10 4 $OUTPUTDIR 3 1,2 no GFAL $PROJDIR"

# use --quantity instead of jobis
INITPARMS="--quantity --parallel --wait --outputpath '|--dump|--xrootd' --checksum --command --dir"
feedParms "1 10 4 $OUTPUTDIR 1 yes LCG $PROJDIR"
feedParms "2 10 4 $OUTPUTDIR 1 yes GFAL $PROJDIR"
feedParms "3 10 4 $OUTPUTDIR 1 no GFAL $PROJDIR"
feedParms "4 4 4 $OUTPUTDIR 2 yes LCG $PROJDIR"

for param in "${USETHISPARMS[@]}";do
  checkThisCommand getoutput "$param"
done

#report:
# crab report
# --outputdir --proxy --dir
USETHISPARMS=()
INITPARMS="--outputdir --proxy --dir"
feedParms "$OUTPUTDIR $PROXY $PROJDIR"
feedParms "$OUTPUTDIR $PROXY $PROJDIR" 
for param in "${USETHISPARMS[@]}";do
  checkThisCommand report "$param"
done

# to generate a new template for all crab commands and options you can use the code below:
#
# ncmds=(`crab --help | sed -n '/Valid commands are/,/To get single command help run/p' | awk '{print $1}'`)
# ncmds=(${ncmds[@]:1:$((${#ncmds[@]}-2))})
# for op in ${ncmds[@]};do echo "### options for crab $op"; echo -ne "INITPARMS=\"`crab $op -h | sed -n '/--help/,$p' | grep '.*=.*' | sed -n 's|.*\(--.*\)=.*|\1|p' | awk '{print $1}' | xargs | sed 's/-h,//g'`\"\n\n";done
#
#
# just keep in mind that some options are mutually excluding, for example for crab tasks, you can type:
#   crab tasks --fromdate=<YYYY-MM-DD>
#      or 
#   crab tasks --day=<YYYY-MM-DD>
# but never 
#   crab tasks --fromday=<YYYY-MM-DD> --day=<YYYY-MM-DD>
# 


# this test needs a taskname submitted with --dryrun option
# crab proceed
#INITPARMS="--proxy --dir --instance"

# this one needs a taskname to be executed
# crab remake
#remake:
#TASKNAME=`crab tasks --days=1 | grep '_crab_'`
TASKNAME=(`crab tasks | grep '_crab_'`)
RMKDIR=`echo $TASKNAME | sed -n 's|.*_\(crab_.*\)|\1|p'`
RMKDIR=`echo "${TASKNAME[@]}" | xargs -n1 | sed -n 's|.*_\(crab_.*\)|\1|p'`
RMKDIR=remake

USETHISPARMS=()
INITPARMS="--task --proxy"
for task in ${TASKNAME[@]};do
  feedParms "$task $PROXY"
done

#feedParms "$RMKDIR $PROXY"

if [ ! -d "./remake" ];then
  mkdir remake
fi 

cd remake

for param in "${USETHISPARMS[@]}";do
  checkThisCommand remake "${param}"
done

cd ..

# this one needs a taskname which ran into a FAILED status
# crab resubmit
USETHISPARMS=()
INITPARMS="--jobids --sitewhitelist --siteblacklist --maxjobruntime --maxmemory --numcores --priority --wait --force --publication --proxy --dir --instance"

# crab uploadlog
USETHISPARMS=()
INITPARMS="--logpath --proxy --dir --instance"

# crab purge
USETHISPARMS=()
INITPARMS="--schedd --cache --proxy --dir --instance"

# for this test is better to submit a new task wait
# few minutes and then issue a kill 
# crab kill
USETHISPARMS=()
INITPARMS="--jobids --proxy --dir --instance"

} 2>&1 | tee client-validation-`date "+%s"`.log

