#!/bin/sh
#
# Script will submit all tasks with set parameters#
#

#Parameters required to change !
#------------------------------
#$TAG $VERSION $CMSSW_VERSION $SCRAMARCH $WORK_DIR $MAIN_DIR $CRAB_SOURCE_SCRIPT $CLIENT $STORAGE_SITE $INSTANCE

echo
echo "---Entering submit environment"

TAG=$1
VERSION=$2
CMSSW_VERSION=$3
SCRAMARCH=$4
WORK_DIR=$5
MAIN_DIR=$6
CRAB_SOURCE_SCRIPT=$7
CLIENT=$8
STORAGE_SITE=$9
INSTANCE=${10}

#Move sed to function, no need to repeat it several times
function sed_new_data () {
  sed --in-place "s|\.General\.requestName = .*|\.General\.requestName = '$2' |" $1
  sed --in-place "s|\.General\.transferOutputs = .*|\.General\.transferOutputs = $3 |" $1
  sed --in-place "s|\.General\.transferLogs = .*|\.General\.transferLogs = $4 |" $1
  sed --in-place "s|\.General\.workArea = .*|\.General\.workArea = '$5' |" $1
  #Publication part
  sed --in-place "s|\.Data\.publication = .*|\.Data\.publication = $6 |" $1
  sed --in-place "s|\.Data\.outputDatasetTag = .*|\.Data\.outputDatasetTag = '$7' |" $1
  #Site part
  sed --in-place "s|\.Site\.storageSite = .*|\.Site\.storageSite = '$STORAGE_SITE' |" $1
  #Data part
  sed --in-place "s|\.Data\.ignoreLocality = .*|\.Data\.ignoreLocality = $8 |" $1
  sed --in-place "s|\.General\.instance = .*|\.General\.instance = '$INSTANCE' |" $1
  sed --in-place "s|\.JobType\.disableAutomaticOutputCollection = .*|\.JobType\.disableAutomaticOutputCollection = $9 |" $1
}

function sed_add_data () {
  sed --in-place "s|\.JobType\.maxJobRuntimeMin = .*|\.JobType\.maxJobRuntimeMin = $2 |" $1
  sed --in-place "s|\.JobType\.maxMemoryMB = .*|\.JobType\.maxMemoryMB = $3 |" $1
  sed --in-place "s|\.JobType\.numCores = .*|\.JobType\.numCores = $4 |" $1
  sed --in-place "s|\.User\.voRole = .*|\.User\.voRole = $5 |" $1
  sed --in-place "s|\.User\.voGroup = .*|\.User\.voGroup = $6 |" $1
  if $7; then
    sed -i -e "/HammerCloud/s/^#//" $file_name
  else
    sed -i -e "/HammerCloud/s/^#*/#/" $file_name
  fi
  if $8; then
    sed -i -e "/stageout/s/^#//" $file_name
  else
    sed -i -e "/stageout/s/^#*/#/" $file_name
  fi
}

if [ ! -d $WORK_DIR ];then
  mkdir -p $WORK_DIR
fi

cd $WORK_DIR

if [ ! -d logs ];then
  mkdir logs
fi

LOGFILE="$WORK_DIR/logs/$CMSSW_VERSION-on-$SCRAMARCH.log"
LOGFILE="./$CMSSW_VERSION-on-$SCRAMARCH.log"


# create a working dir
if [ ! -d CMSSW_TESTS ];then
   mkdir CMSSW_TESTS
fi

echo
echo '==================== INITIAL ENVIRONMENT ========================='
env
echo '=========================================================='
echo

# this is needed because we have a bare minimun environment
source /afs/cern.ch/cms/cmsset_default.sh
file_name='MinBias_PrivateMC_EventBased_CMSSW_version.py'

#echo "Testing $CMSSW_VERSION on $SCRAMARCH"
export SCRAM_ARCH=$SCRAMARCH

cd $WORK_DIR/CMSSW_TESTS

if [ ! -d $CMSSW_VERSION ];then
  cmsrel $CMSSW_VERSION #2>&1 >
fi

cd $CMSSW_VERSION/src
cmsenv

#load crab client environment
echo "---Sourcing the client"

source $CLIENT



#echo '==================== FINAL ENVIRONMENT ========================='
#env
#echo '=========================================================='

if [ ! -d psets ];then
  mkdir psets
fi

cp -R $MAIN_DIR/config/MinBias_PrivateMC_EventBased_CMSSW_version.py ./
cp -R $MAIN_DIR/config/psets/pset_tutorial_MC_generation.py ./psets

file_name_temp=${file_name:0:(${#file_name})-3}

#Generate new name
new_name=$TAG-${CMSSW_VERSION}-$VERSION-${SCRAMARCH}-$file_name_temp
publish_name=$new_name-`date +%s`
echo $new_name
sed_new_data $file_name $new_name True True $TAG-$VERSION True $publish_name False False
crab submit -c $file_name
#2>&1 >

echo
echo "---Leaving submit environment"
echo
echo
