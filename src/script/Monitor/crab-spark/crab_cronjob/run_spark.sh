#!/bin/bash

# # source the environment for spark submit
# kinit cmscrab@CERN.CH -k -t /workdir/cmscrab.keytab
# source hadoop-setconf.sh analytix 3.2 spark3
# export PYSPARK_PYTHON=/cvmfs/sft.cern.ch/lcg/releases/Python/3.9.6-b0f98/x86_64-centos7-gcc8-opt/bin/python3
# source /cvmfs/sft.cern.ch/lcg/views/LCG_103swan/x86_64-centos7-gcc11-opt/setup.sh
# python3 -m pip install opensearch-py

cd /workdir/crab-dp3/crab_cronjob/

source ./bootstrap.sh

# submit $1 to spark, where $1 supposes to be a data pulling file (.py)
spark-submit --master yarn --packages org.apache.spark:spark-avro_2.12:3.3.1 $1
