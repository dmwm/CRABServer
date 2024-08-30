#!/bin/bash

# work directory
cd /data/srv/spark

# source the environment for spark submit
source ./bootstrap.sh

# submit $1 to spark, where $1 supposes to be a data pulling file (.py)
spark-submit --master yarn --packages org.apache.spark:spark-avro_2.12:3.5.0 $@
