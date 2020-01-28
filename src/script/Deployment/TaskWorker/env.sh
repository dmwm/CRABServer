scram_arch=slc7_amd64_gcc630


export MYTESTAREA=/data/srv/TaskManager/current
source $MYTESTAREA/${scram_arch}/cms/crabtaskworker/*/etc/profile.d/init.sh

export CRABTASKWORKER_ROOT
export CRABTASKWORKER_VERSION
export CONDOR_CONFIG=/data/srv/condor_config
