#!/bin/bash
# find my bearings
thisScript=`realpath $0`
myDir=`dirname ${thisScript}`
myLog=${myDir}/logs/log.txt

PublisherBusy(){
# a function to tell if PublisherMaster is busy or waiting
#   return 0 = success = Publisher is Busy
#   return 1 = failure = Publisher can be killed w/o fear
  lastLine=`tail -1 ${myLog}`
  echo ${lastLine}|grep -q 'Next cycle will start at'
  cycleDone=$?
  if [ $cycleDone = 1 ] ; then
    # inside working cycle
    return 0
  else
    # in waiting mode, checks when it is next start
    start=`echo $lastLine|awk '{print $NF}'`
    startTime=`date -d ${start} +%s`  # in seconds from Epoch
    now=`date +%s` # in seconds from Epoch
    delta=$((${startTime}-${now}))
    if [ $delta -gt 60 ]; then
      # no race with starting of next cycle, safe to kill
      return 1
    else
      # next cycle about to start, wait until is done
      return 0
    fi
  fi
}

nIter=1
while PublisherBusy
do
  [ $nIter = 1 ] && echo "Waiting for MasterPublisher to complete cycle: ."
  nIter=$((nIter+1))
  sleep 10
  echo -n "."
  if (( nIter%6  == 0 )); then
    minutes=$((nIter/6))
    echo -n "${minutes}m"
  fi
done
echo ""
echo "Publisher is in waiting now. Killing RunPublisher"
pkill -f RunPublisher
