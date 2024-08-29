#!/bin/bash

TAG=latest
if [[ -n $1 ]]; then
  TAG=$1
fi

docker run --rm --net=host -v /cvmfs:/cvmfs:shared \
      -v /data/certs/monit.d/monit_spark_crab.txt:/data/certs/monit.d/monit_spark_crab.txt \
      -v /data/certs/keytabs.d/cmscrab.keytab:/data/certs/keytabs.d/cmscrab.keytab \
      registry.cern.ch/cmscrab/crabspark:${TAG} \
      bash /data/srv/spark/run_spark.sh /data/srv/spark/crab_data_daily.py \

docker run --rm --net=host -v /cvmfs:/cvmfs:shared \
      -v /data/certs/monit.d/monit_spark_crab.txt:/data/certs/monit.d/monit_spark_crab.txt \
      -v /data/certs/keytabs.d/cmscrab.keytab:/data/certs/keytabs.d/cmscrab.keytab \
      registry.cern.ch/cmscrab/crabspark:${TAG} \
      bash /data/srv/spark/run_spark.sh /data/srv/spark/crab_condor_daily.py

 docker run --rm --net=host -v /cvmfs:/cvmfs:shared \
      -v /data/certs/monit.d/monit_spark_crab.txt:/data/certs/monit.d/monit_spark_crab.txt \
      -v /data/certs/keytabs.d/cmscrab.keytab:/data/certs/keytabs.d/cmscrab.keytab \
      registry.cern.ch/cmscrab/crabspark:${TAG} \
      bash /data/srv/spark/run_spark.sh /data/srv/spark/crab_tape_recall_rules_history_daily.py 

docker run --rm --net=host -v /cvmfs:/cvmfs:shared \
  -v /data/certs/monit.d/monit_spark_crab.txt:/data/certs/monit.d/monit_spark_crab.txt \
  -v /data/certs/keytabs.d/cmscrab.keytab:/data/certs/keytabs.d/cmscrab.keytab \
  registry.cern.ch/cmscrab/crabspark:${TAG} \
  bash /data/srv/spark/run_spark.sh /data/srv/spark/crab_tape_recall_updated_rules_daily.py

