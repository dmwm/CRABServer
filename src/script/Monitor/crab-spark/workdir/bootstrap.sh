# source the environment for spark submit
kinit cmscrab@CERN.CH -k -t /data/certs/keytabs.d/cmscrab.keytab
source hadoop-setconf.sh analytix 

LCG_VER=/cvmfs/sft.cern.ch/lcg/views/LCG_105a_swan/x86_64-el9-gcc13-opt
source  $LCG_VER/setup.sh
export PYSPARK_PYTHON=$LCG_VER/bin/python3

# i know, ugly, we should install software in the dockerfile
# however, we really need an environment from cvmfs, and i am not sure we 
# can have access to cvmfs at build time in gitlab
python3 -m pip install --user opensearch-py

# finish the environment
export CRAB_KRB5_USERNAME=$(klist | grep -i Default | cut -d":" -f2 | cut -d"@" -f"1" | awk '{$1=$1};1')

