#!/bin/bash

docker run --rm --net=host -v /cvmfs:/cvmfs:shared -v $HOME/workdir:/workdir \
  registry.cern.ch/cmsmonitoring/cmsmon-spark:v0.4.1.10 \
  /workdir/crab-dp3/crab_cronjob/run_spark.sh /workdir/crab-dp3/crab_cronjob/crab_data_daily.py

docker run --rm --net=host -v /cvmfs:/cvmfs:shared -v $HOME/workdir:/workdir \
  registry.cern.ch/cmsmonitoring/cmsmon-spark:v0.4.1.10 \
  /workdir/crab-dp3/crab_cronjob/run_spark.sh /workdir/crab-dp3/crab_cronjob/crab_tape_recall_updated_rules_daily.py

docker run --rm --net=host -v /cvmfs:/cvmfs:shared -v $HOME/workdir:/workdir \
  registry.cern.ch/cmsmonitoring/cmsmon-spark:v0.4.1.10 \
  /workdir/crab-dp3/crab_cronjob/run_spark.sh /workdir/crab-dp3/crab_cronjob/crab_tape_recall_rules_history_daily.py

docker run --rm --net=host -v /cvmfs:/cvmfs:shared -v $HOME/workdir:/workdir \
  registry.cern.ch/cmsmonitoring/cmsmon-spark:v0.4.1.10 \
  /workdir/crab-dp3/crab_cronjob/run_spark.sh /workdir/crab-dp3/crab_cronjob/crab_condor_daily.py
