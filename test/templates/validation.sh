#
# Script will submit all tasks with set parameters#
#

#Parameters required to change !
#------------------------------
TAG='HG1407a'
VERSION=1
CMSSW='CMSSW_7_0_6'
WORK_DIR=~/VALIDATION/$TAG
MAIN_DIR=`pwd`
CLIENT=/cvmfs/cms.cern.ch/crab3/crab_pre.sh
STORAGE_SITE='T2_CH_CERN'
#-----------------------------

source /afs/cern.ch/cms/cmsset_default.sh
#Specify environment variable, change it if it`s required
export SCRAM_ARCH=slc6_amd64_gcc481

# out_cond - Output conditions, log_cond - Logs condition, pub_cond - Publication cond
# Number of possible values should be always equal
out_cond=(True True True False True False)
log_cond=(True True False False False True)
pub_cond=(True False False False True False)

#Get specified CMSSW
#Need to have a check if directory exists or not, if not do cmsrel
mkdir -p $WORK_DIR
cd $WORK_DIR
cmsrel $CMSSW
cd $WORK_DIR/$CMSSW/src/

#Setup CMS environment
cmsenv

#Source crab client
#This is required until it will be solved and added crabclient in CMSSW
source $CLIENT

cp -R $MAIN_DIR/config/* .

#untar skimming and do scram b
tar -xvf SkimMsecSleep.tar
scram b

for file_name in `find . -maxdepth 1 -name '*.py'`;
do
  echo $file_name;
  file_name_temp=${file_name:2:(${#file_name})-5};
  total=${#out_cond[*]}
  for ((i=0;i<=$(($total-1));i++));
  do
    out_v=${out_cond[$i]:0:1}
    log_v=${log_cond[$i]:0:1}
    pub_v=${pub_cond[$i]:0:1}

#Generate new name
    new_name=$TAG-$VERSION-$file_name_temp-'L-'$log_v'_O-'$out_v'_P-'$pub_v
    publish_name=$new_name-`date +%s`
    echo $new_name
#General part
    sed --in-place "s|\.General\.requestName = .*|\.General\.requestName = '$new_name'|" $file_name
    sed --in-place "s|\.General\.transferOutput = .*|\.General\.transferOutput = ${out_cond[$i]} |" $file_name
    sed --in-place "s|\.General\.saveLogs = .*|\.General\.saveLogs = ${log_cond[$i]} |" $file_name
    sed --in-place "s|\.General\.workArea = .*|\.General\.workArea = '$TAG-$VERSION' |" $file_name
    #Publication part
    sed --in-place "s|\.Data\.publication = .*|\.Data\.publication = ${pub_cond[$i]} |" $file_name
    sed --in-place "s|\.Data\.publishDataName = .*|\.Data\.publishDataName = '$publish_name' |" $file_name
    #Site part
    sed --in-place "s|\.Site\.storageSite = .*|\.Site\.storageSite = '$STORAGE_SITE' |" $file_name
    crab submit -c $file_name
  done
done
