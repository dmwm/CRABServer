#
# Script will submit all tasks with set parameters#
#

#Parameters required to change !
#------------------------------
TAG='HG1501'
VERSION=1
CMSSW='CMSSW_7_0_6'
WORK_DIR=/afs/cern.ch/work/j/jbalcas/VALIDATE/$TAG
MAIN_DIR=`pwd`
CLIENT=/cvmfs/cms.cern.ch/crab3/crab_pre.sh
STORAGE_SITE='T2_CH_CERN'
INSTANCE='preprod'
#-----------------------------

source /afs/cern.ch/cms/cmsset_default.sh
#Specify environment variable, change it if it`s required
export SCRAM_ARCH=slc6_amd64_gcc481

# out_cond - Output conditions, log_cond - Logs condition, pub_cond - Publication cond, ign_cond - IgnoreLocality
# Number of possible values should be always equal
out_cond=(True True True True False False False)
log_cond=(True True True False False True True)
pub_cond=(True True False False False True False)
ign_cond=(True False False False True False False)

#Move sed to function, no need to repeat it several times
function sed_new_data () {
  sed --in-place "s|\.General\.requestName = .*|\.General\.requestName = '$2' |" $1
  sed --in-place "s|\.General\.transferOutputs = .*|\.General\.transferOutputs = $3 |" $1
  sed --in-place "s|\.General\.transferLogs = .*|\.General\.transferLogs = $4 |" $1
  sed --in-place "s|\.General\.workArea = .*|\.General\.workArea = '$5' |" $1
  #Publication part
  sed --in-place "s|\.Data\.publication = .*|\.Data\.publication = $6 |" $1
  sed --in-place "s|\.Data\.publishDataName = .*|\.Data\.publishDataName = '$7' |" $1
  #Site part
  sed --in-place "s|\.Site\.storageSite = .*|\.Site\.storageSite = '$STORAGE_SITE' |" $1
  #Data part
  sed --in-place "s|\.Data\.ignoreLocality = .*|\.Data\.ignoreLocality = $8 |" $1
  sed --in-place "s|\.General\.instance = .*|\.General\.instance = '$INSTANCE' |" $1
}


#Get specified CMSSW
if [ ! -d "$WORK_DIR" ]; then
  mkdir -p $WORK_DIR
fi
cd $WORK_DIR
if [ ! -d "$CMSSW" ]; then
  cmsrel $CMSSW
fi
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

#No point testing all templates with possible versions of flags
#Change 2015.01 only for MinBias_PrivateMC
file_name='MinBias_PrivateMC_EventBased.py'
file_name_temp=${file_name:0:(${#file_name})-3};
total=${#out_cond[*]}
for ((i=0;i<=$(($total-1));i++));
do
  out_v=${out_cond[$i]:0:1}
  log_v=${log_cond[$i]:0:1}
  pub_v=${pub_cond[$i]:0:1}
  ign_v=${ign_cond[$i]:0:1}

  #Generate new name
  new_name=$TAG-$VERSION-$file_name_temp-'L-'$log_v'_O-'$out_v'_P-'$pub_v'_IL-'$ign_v
  publish_name=$new_name-`date +%s`
  echo $new_name
  sed_new_data $file_name $new_name ${out_cond[$i]} ${log_cond[$i]} $TAG-$VERSION ${pub_cond[$i]} $publish_name ${ign_cond[$i]}
  crab submit -c $file_name
done

#Submit all left templates
for file_name in `find . -maxdepth 1 -name '*.py'`;
do
  echo $file_name;
  file_name_temp=${file_name:2:(${#file_name})-5};
  #Generate new name
  new_name=$TAG-$VERSION-$file_name_temp-'L-T_O-T_P-T_IL-F'
  publish_name=$new_name-`date +%s`
  echo $new_name
  sed_new_data $file_name $new_name True True $TAG-$VERSION True $publish_name False
  crab submit -c $file_name
done


#As for LHE it requires older version of CMSSW. Need to ask Anna to review, she might know for newer version of CMSSW
CMSSW='CMSSW_5_3_22'
cd $WORK_DIR
if [ ! -d "$CMSSW" ]; then
  cmsrel $CMSSW
fi
cd $WORK_DIR/$CMSSW/src/
#Setup CMS environment
cmsenv

#Source crab client
#This is required until it will be solved and added crabclient in CMSSW
source $CLIENT

cp -R $MAIN_DIR/config/* .

#Copy LHE file from AFS (Size too big for GIT)
cp /afs/cern.ch/user/j/jbalcas/public/forLHE/dynlo.lhe input_files/

file_name='PrivateMC_for_LHE.py'
file_name_temp=${file_name:0:(${#file_name})-3};
#Generate new name
new_name=$TAG-$VERSION-$file_name_temp-'L-T_O-T_P-T_IL-F'

sed_new_data $file_name $new_name True True $TAG-$VERSION True $publish_name False
crab submit -c $file_name


#Use Parent
file_name='Analysis_Use_Parent.py'
file_name_temp=${file_name:0:(${#file_name})-3};
#Generate new name
new_name=$TAG-$VERSION-$file_name_temp-'L-T_O-T_P-T_IL-F'

sed_new_data $file_name $new_name True True $TAG-$VERSION True $publish_name False
crab submit -c $file_name
