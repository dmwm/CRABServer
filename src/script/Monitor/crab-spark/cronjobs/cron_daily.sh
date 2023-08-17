#!/bin/bash

docker run --rm --net=host -v /cvmfs:/cvmfs:shared -v $HOME/workdir:/workdir \
  registry.cern.ch/cmsmonitoring/cmsmon-spark:v0.4.1.10 \
  /workdir/CRABServer/src/script/Monitor/crab-spark/crab_cronjob/run_spark.sh /workdir/CRABServer/src/script/Monitor/crab-spark/crab_cronjob/crab_data_daily.py

docker run --rm --net=host -v /cvmfs:/cvmfs:shared -v $HOME/workdir:/workdir \
  registry.cern.ch/cmsmonitoring/cmsmon-spark:v0.4.1.10 \
  /workdir/CRABServer/src/script/Monitor/crab-spark/crab_cronjob/run_spark.sh /workdir/CRABServer/src/script/Monitor/crab-spark/crab_cronjob/crab_tape_recall_updated_rules_daily.py

docker run --rm --net=host -v /cvmfs:/cvmfs:shared -v $HOME/workdir:/workdir \
  registry.cern.ch/cmsmonitoring/cmsmon-spark:v0.4.1.10 \
  /workdir/CRABServer/src/script/Monitor/crab-spark/crab_cronjob/run_spark.sh /workdir/CRABServer/src/script/Monitor/crab-spark/crab_cronjob/crab_tape_recall_rules_history_daily.py

docker run --rm --net=host -v /cvmfs:/cvmfs:shared -v $HOME/workdir:/workdir \
  registry.cern.ch/cmsmonitoring/cmsmon-spark:v0.4.1.10 \
  /workdir/CRABServer/src/script/Monitor/crab-spark/crab_cronjob/run_spark.sh /workdir/CRABServer/src/script/Monitor/crab-spark/crab_cronjob/crab_condor_daily.py
