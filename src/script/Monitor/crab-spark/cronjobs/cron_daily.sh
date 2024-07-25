#!/bin/bash

## this file is deprecated, it is kept around for reference and historycal reasons

docker run --rm --net=host -v /cvmfs:/cvmfs:shared -v $HOME/workdir:/workdir \
  -v /data/certs/monit.d/monit_spark_crab.txt:/data/certs/monit.d/monit_spark_crab.txt \
  -v /data/certs/keytabs.d/cmscrab.keytab:/data/certs/keytabs.d/cmscrab.keytab \
  registry.cern.ch/cmsmonitoring/cmsmon-spark:v0.5.0.1 \
  bash /workdir/CRABServer/src/script/Monitor/crab-spark/cronjobs/run_spark.sh /workdir/CRABServer/src/script/Monitor/crab-spark/cronjobs/crab_data_daily.py

docker run --rm --net=host -v /cvmfs:/cvmfs:shared -v $HOME/workdir:/workdir \
  -v /data/certs/monit.d/monit_spark_crab.txt:/data/certs/monit.d/monit_spark_crab.txt \
  -v /data/certs/keytabs.d/cmscrab.keytab:/data/certs/keytabs.d/cmscrab.keytab \
  registry.cern.ch/cmsmonitoring/cmsmon-spark:v0.5.0.1 \
  bash /workdir/CRABServer/src/script/Monitor/crab-spark/cronjobs/run_spark.sh /workdir/CRABServer/src/script/Monitor/crab-spark/cronjobs/crab_tape_recall_updated_rules_daily.py

docker run --rm --net=host -v /cvmfs:/cvmfs:shared -v $HOME/workdir:/workdir \
  -v /data/certs/monit.d/monit_spark_crab.txt:/data/certs/monit.d/monit_spark_crab.txt \
  -v /data/certs/keytabs.d/cmscrab.keytab:/data/certs/keytabs.d/cmscrab.keytab \
  registry.cern.ch/cmsmonitoring/cmsmon-spark:v0.5.0.1 \
  bash /workdir/CRABServer/src/script/Monitor/crab-spark/cronjobs/run_spark.sh /workdir/CRABServer/src/script/Monitor/crab-spark/cronjobs/crab_tape_recall_rules_history_daily.py

docker run --rm --net=host -v /cvmfs:/cvmfs:shared -v $HOME/workdir:/workdir \
  -v /data/certs/monit.d/monit_spark_crab.txt:/data/certs/monit.d/monit_spark_crab.txt \
  -v /data/certs/keytabs.d/cmscrab.keytab:/data/certs/keytabs.d/cmscrab.keytab \
  registry.cern.ch/cmsmonitoring/cmsmon-spark:v0.5.0.1 \
  bash /workdir/CRABServer/src/script/Monitor/crab-spark/cronjobs/run_spark.sh /workdir/CRABServer/src/script/Monitor/crab-spark/cronjobs/crab_condor_daily.py
