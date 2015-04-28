#
# Script will submit all tasks with set parameters#
#

#Parameters required to change !
#------------------------------
TAG='HG1505b'
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
  sed_new_data $file_name $new_name ${out_cond[$i]} ${log_cond[$i]} $TAG-$VERSION ${pub_cond[$i]} $publish_name ${ign_cond[$i]} False
  crab submit -c $file_name
  #Submit same task with disableAutomaticOutputCollection = True
  new_name=$new_name-'DOC-T'
  sed_new_data $file_name $new_name ${out_cond[$i]} ${log_cond[$i]} $TAG-$VERSION ${pub_cond[$i]} $publish_name ${ign_cond[$i]} True
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
  sed_new_data $file_name $new_name True True $TAG-$VERSION True $publish_name False False
  crab submit -c $file_name
  #Submit same task with disableAutomaticOutputCollection = True
  new_name=$new_name-'DOC-T'
  sed_new_data $file_name $new_name True True $TAG-$VERSION True $publish_name False True
  crab submit -c $file_name
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

sed_new_data $file_name $new_name True True $TAG-$VERSION True $publish_name False False
crab submit -c $file_name

#Submit same task with disableAutomaticOutputCollection = True
new_name=$new_name-'DOC-T'
sed_new_data $file_name $new_name True True $TAG-$VERSION True $publish_name False True
crab submit -c $file_name


#Use Parent
file_name='Analysis_Use_Parent.py'
file_name_temp=${file_name:0:(${#file_name})-3};
#Generate new name
new_name=$TAG-$VERSION-$file_name_temp-'L-T_O-T_P-T_IL-F'

sed_new_data $file_name $new_name True True $TAG-$VERSION True $publish_name False False
crab submit -c $file_name

#Submit same task with disableAutomaticOutputCollection = True
new_name=$new_name-'DOC-T'
sed_new_data $file_name $new_name True True $TAG-$VERSION True $publish_name False True
crab submit -c $file_name
