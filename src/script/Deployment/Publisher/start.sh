#!/bin/bash

helpFunction(){
  echo -e "\nUsage example: ./start.sh -c | -g [-d]"
  echo -e "\t-c start current Publisher instance"
  echo -e "\t-g start Publisher instance from GitHub repo"
  echo -e "\t-d start Publisher in debug mode. Option can be combined with -c or -g"
  exit 1
  }

while getopts ":dDcCgGhH" opt
do
    case "$opt" in
      h|H) helpFunction ;;
      g|G) MODE="private" ;;
      c|C) MODE="current" ;;
      d|D) debug=true ;;
      * ) echo "Unimplemented option: -$OPTARG"; helpFunction ;;
    esac
done

if ! [ -v MODE ]; then
  echo "Please set how you want to start Publisher (add -c or -g option)." && helpFunction
fi

unset X509_USER_PROXY
unset X509_USER_CERT
unset X509_USER_KEY

rm -f nohup.out

# if GH repositories location is not already defined, set a default
if ! [ -v GHrepoDir ]
then
  GHrepoDir='/data/hostdisk/repos'
fi

__strip_pythonpath(){
  # this function is used to strip the taskworker lines from $PYTHONPATH
  # in order for the debug |private calls to be able to add theirs

  local strip_reg=".*crabtaskworker.*"
  local ppath_init=${PYTHONPATH//:/: }
  local ppath_stripped=""

  for i in $ppath_init
  do
      [[ $i =~ $strip_reg ]] || ppath_stripped="${ppath_stripped}${i}"
  done
  # echo -e "before strip: \n$ppath_init" |sed -e 's/\:/\:\n/g'
  # echo -e "after strip: \n$ppath_stripped" |sed -e 's/\:/\:\n/g'
  export PYTHONPATH=$ppath_stripped
}

# if PUBLISHER_HOME is already defined, use it
if [ -v PUBLISHER_HOME ]
then
  echo "PUBLISHER_HOME already set to $PUBLISHER_HOME. Will use that"
else
  thisScript=`realpath $0`
  myDir=`dirname ${thisScript}`
  export PUBLISHER_HOME=${myDir}  # where we run the Publisher and where Config is
  echo "Define environment for Publisher in $PUBLISHER_HOME"
fi

source ${PUBLISHER_HOME}/env.sh
ln -s /data/hostdisk/${SERVICE}/nohup.out nohup.out

case $MODE in
  private)
    # private mode: run private instance from ${GHrepoDir}
    __strip_pythonpath
    export PYTHONPATH=${GHrepoDir}/CRABServer/src/python:${GHrepoDir}/WMCore/src/python:$PYTHONPATH
    if [ "$debug" = true ]; then
      python -m pdb ${GHrepoDir}/CRABServer/src/python/Publisher/SequentialPublisher.py --config $PUBLISHER_HOME/PublisherConfig.py --debug
    else
      nohup python ${GHrepoDir}/CRABServer/src/python/Publisher/PublisherMaster.py --config $PUBLISHER_HOME/PublisherConfig.py &
    fi
  ;;
  current)
  # current mode: run current instance
    if [ "$debug" = true ]; then
      python $PUBLISHER_ROOT/lib/python2.7/site-packages/Publisher/SequentialPublisher.py --config $PUBLISHER_HOME/PublisherConfig.py --debug
    else
      nohup python $PUBLISHER_ROOT/lib/python2.7/site-packages/Publisher/PublisherMaster.py --config $PUBLISHER_HOME/PublisherConfig.py &
    fi
esac

