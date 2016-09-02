TAG='HG1607i'
VERSION='rc1'
CMSSW='CMSSW_7_4_7' # The template for UseSecondary dataset needs this version
WORK_DIR="/afs/cern.ch/user/j/jmsilva/workspace/crabValidation/$TAG"
MAIN_DIR="/afs/cern.ch/user/j/jmsilva/workspace/crabValidation/scripts"
FAILED_TASKS="failed_tasks_${TAG}-${VERSION}.log"

# from HG1605 we start using light client
export CRAB_SOURCE_SCRIPT='/cvmfs/cms.cern.ch/crab3/crab_pre.sh'

#light client
#CLIENT='/cvmfs/cms.cern.ch/crab3/crab_pre_light.sh'

# we use our own crab client until we had cvmfs one fixed to
# work with scripts
#CLIENT="$MAIN_DIR/crab_light2.sh"

# normal pre prod client
CLIENT='/cvmfs/cms.cern.ch/crab3/crab_pre.sh'

STORAGE_SITE='T2_CH_CERN'
INSTANCE='preprod'
#-----------------------------

source /afs/cern.ch/cms/cmsset_default.sh
#Specify environment variable, change it if it`s required
export SCRAM_ARCH=slc6_amd64_gcc491

#-----------------------------
echo ============================
echo "TAG AND VERSION: $TAG-$VERSION"
echo "WORK_DIR: $WORK_DIR"
echo "MAIN_DIR: $MAIN_DIR"
echo "CLIENT: $CLIENT"
echo ============================

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


