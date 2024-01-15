#!/bin/bash

## bash ./cicd/build/monit-python/monit_build.sh

cp ./scripts/Utils/CheckTapeRecall.py      ./cicd/build/monit-python/
cp ./src/python/ServerUtilities.py         ./cicd/build/monit-python/
cp ./src/python/RESTInteractions.py        ./cicd/build/monit-python/
cp ./src/script/Monitor/aso_metrics_ora.py ./cicd/build/monit-python/
cp ./src/script/Monitor/ReportRecallQuota.py ./cicd/build/monit-python/

docker build \
  --network=host \
  -t registry.cern.ch/cmscrab/crabtaskworker:20240328-monitpython \
  ./cicd/build/monit-python
