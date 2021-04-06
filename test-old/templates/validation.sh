#!/bin/sh
#
# Script will submit all tasks with set parameters#
#

#Parameters required to change !
#------------------------------
source ./validation-args.sh

#Get specified CMSSW
if [ ! -d "$WORK_DIR" ]; then
  mkdir -p $WORK_DIR
fi
cd $WORK_DIR
if [ ! -d "$CMSSW" ]; then
  cmsrel $CMSSW
fi

# ~/workspace/crabValidation/
cd $WORK_DIR/$CMSSW/src/

#Setup CMS environment
cmsenv

#Source crab client
#This is required until it will be solved and added crabclient in CMSSW
source $CLIENT

echo "Using CRAB version:"
crab --version

cp -R $MAIN_DIR/config/* .

#untar skimming and do scram b
tar -xvf SkimMsecSleep.tar
scram b

#files_to_exclude="./MinBias_PrivateMC_EventBased_ExtraParams.py ./MinBias_PrivateMC_EventBased.py ./PrivateMC_for_LHE.py ./MinBias_PrivateMC_EventBased_Sites.py"
files_to_exclude="./MinBias_PrivateMC_EventBased_ExtraParams.py ./MinBias_PrivateMC_EventBased.py ./MinBias_PrivateMC_EventBased_Sites.py ./MinBias_PrivateMC_EventBased_CMSSW_version.py"

if [ "X$1" == "X" ];then
  echo "running default templates"
else
  echo "running only the template $1"
  file_name=$1

  re=.*${file_name}.*
  if [[ ${files_to_exclude[*]} =~ $re ]]; then
    echo "-"
    echo "-Skipping $file_name because it is already tested or will be after regular templates."
    echo "-"
    exit
  fi

  echo $file_name;
  file_name_temp=${file_name:0:(${#file_name})-3};
  #Generate new name
  new_name=$TAG-$VERSION-$file_name_temp-'L-T_O-T_P-T_IL-F'
  publish_name=$new_name-`date +%s`
  echo $new_name
  sed_new_data $file_name $new_name True True $TAG-$VERSION True $publish_name False False
  crab submit -c $file_name
  if [ $? <> 0 ];then
    echo new_name >> $FAILED_TASKS
  fi
  #Submit same task with disableAutomaticOutputCollection = True
  new_name=$new_name-'DOC-T'
  sed_new_data $file_name $new_name True True $TAG-$VERSION True $publish_name False True
  crab submit -c $file_name
  if [ $? <> 0 ];then
    echo new_name >> $FAILED_TASKS
  fi
  exit 0
fi


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
  sed_new_data $file_name $new_name ${out_cond[$i]} ${log_cond[$i]} $TAG-$VERSION ${pub_cond[$i]} $publish_name ${ign_cond[$i]} False
  crab submit -c $file_name
  if [ $? <> 0 ];then
    echo new_name >> $FAILED_TASKS
  fi
  #Submit same task with disableAutomaticOutputCollection = True
  new_name=$new_name-'DOC-T'
  sed_new_data $file_name $new_name ${out_cond[$i]} ${log_cond[$i]} $TAG-$VERSION ${pub_cond[$i]} $publish_name ${ign_cond[$i]} True
  crab submit -c $file_name
  if [ $? <> 0 ];then
    echo new_name >> $FAILED_TASKS
  fi
done


#Submit all left templates
for file_name in `find . -maxdepth 1 -name '*.py'`;
do

  re=.*${file_name}.*
  if [[ ${files_to_exclude[*]} =~ $re ]]; then
    echo "-"
    echo "-Skipping $file_name because it is already tested or will be after regular templates."
    echo "-"
    continue
  fi

  echo $file_name;
  file_name_temp=${file_name:2:(${#file_name})-5};
  #Generate new name
  new_name=$TAG-$VERSION-$file_name_temp-'L-T_O-T_P-T_IL-F'
  publish_name=$new_name-`date +%s`
  echo $new_name
  sed_new_data $file_name $new_name True True $TAG-$VERSION True $publish_name False False
  crab submit -c $file_name
  if [ $? <> 0 ];then
    echo new_name >> $FAILED_TASKS
  fi
  #Submit same task with disableAutomaticOutputCollection = True
  new_name=$new_name-'DOC-T'
  sed_new_data $file_name $new_name True True $TAG-$VERSION True $publish_name False True
  crab submit -c $file_name
  if [ $? <> 0 ];then
    echo new_name >> $FAILED_TASKS
  fi
done

# Number of possible values should be always equal
add_name=("RuntimeRM" "MemoryRM" "NumCores" "HammerCloud" "Stageout" "voGroupbecms" "voGroupescms" "MemoryBIG" "RuntimeBig")
wall_cond=(1 1000 1000 1000 1000 1000 1000 1000 3000)
mem_cond=(2000 20 2000 2000 2000 2000 2000 4000 2000)
cores_cond=(1 1 2 1 1 1 1 1 1)
group_cond=('""' '""' '""' '""' '""' '"becms"' '"escms"' '""' '""')
role_cond=('""' '""' '""' '""' '""' '""' '""' '""' '""')
hc_cond=(false false false true false false false false false)
st_cond=(false false false false true false false false false)

#Extra parameters test
file_name='MinBias_PrivateMC_EventBased_ExtraParams.py'
file_name_temp=${file_name:0:(${#file_name})-3};

total=${#add_name[*]}
for ((i=0;i<=$(($total-1));i++));
do
  new_name=$TAG-$VERSION-$file_name_temp-'L-T_O-T_P-T-'${add_name[$i]}
  publish_name=$new_name-`date +%s`
  echo $new_name
  sed_new_data $file_name $new_name True True $TAG-$VERSION True $publish_name False False
  sed_add_data $file_name ${wall_cond[$i]} ${mem_cond[$i]} ${cores_cond[$i]} ${role_cond[$i]} ${group_cond[$i]} ${hc_cond[$i]} ${st_cond[$i]}
  crab submit -c $file_name
  if [ $? <> 0 ];then
    echo new_name >> $FAILED_TASKS
  fi
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

#======================================== PrivateMC_for_LHE
#Copy LHE file from AFS (Size too big for GIT)
#cp /afs/cern.ch/user/j/jbalcas/public/forLHE/dynlo.lhe input_files/
file_name='PrivateMC_for_LHE.py'
file_name_temp=${file_name:0:(${#file_name})-3};
#Generate new name
new_name=$TAG-$VERSION-$file_name_temp-'L-T_O-T_P-T_IL-F'

sed_new_data $file_name $new_name True True $TAG-$VERSION True $publish_name False False
crab submit -c $file_name
if [ $? <> 0 ];then
  echo new_name >> $FAILED_TASKS
fi

#Submit same task with disableAutomaticOutputCollection = True
new_name=$new_name-'DOC-T'
sed_new_data $file_name $new_name True True $TAG-$VERSION True $publish_name False True
crab submit -c $file_name
if [ $? <> 0 ];then
  echo new_name >> $FAILED_TASKS
fi



