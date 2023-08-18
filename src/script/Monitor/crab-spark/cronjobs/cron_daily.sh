#!/bin/bash

docker run --rm --net=host -v /cvmfs:/cvmfs:shared -v $HOME/workdir:/workdir \
  registry.cern.ch/cmsmonitoring/cmsmon-spark:v0.4.1.10 \
  bash /workdir/CRABServer/src/script/Monitor/crab-spark/cronjobs/run_spark.sh /workdir/CRABServer/src/script/Monitor/crab-spark/cronjobs/crab_data_daily.py

docker run --rm --net=host -v /cvmfs:/cvmfs:shared -v $HOME/workdir:/workdir \
  registry.cern.ch/cmsmonitoring/cmsmon-spark:v0.4.1.10 \
  bash /workdir/CRABServer/src/script/Monitor/crab-spark/cronjobs/run_spark.sh /workdir/CRABServer/src/script/Monitor/crab-spark/cronjobs/crab_tape_recall_updated_rules_daily.py

docker run --rm --net=host -v /cvmfs:/cvmfs:shared -v $HOME/workdir:/workdir \
  registry.cern.ch/cmsmonitoring/cmsmon-spark:v0.4.1.10 \
  bash /workdir/CRABServer/src/script/Monitor/crab-spark/cronjobs/run_spark.sh /workdir/CRABServer/src/script/Monitor/crab-spark/cronjobs/crab_tape_recall_rules_history_daily.py

docker run --rm --net=host -v /cvmfs:/cvmfs:shared -v $HOME/workdir:/workdir \
  registry.cern.ch/cmsmonitoring/cmsmon-spark:v0.4.1.10 \
  bash /workdir/CRABServer/src/script/Monitor/crab-spark/cronjobs/run_spark.sh /workdir/CRABServer/src/script/Monitor/crab-spark/cronjobs/crab_condor_daily.py
