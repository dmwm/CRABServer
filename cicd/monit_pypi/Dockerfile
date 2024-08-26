ARG BASE_TAG=latest
FROM registry.cern.ch/cmscrab/crabtaskworker:${BASE_TAG}

RUN pip install --user pandas 

RUN mkdir -p /data/srv/monit/
COPY ./src/python/ServerUtilities.py           \
     ./src/python/RESTInteractions.py          \
     ./src/script/Monitor/GenerateMONIT.py     \
     ./scripts/Utils/CheckTapeRecall.py        \
     ./src/script/Monitor/aso_metrics_ora.py   \
     ./src/script/Monitor/ReportRecallQuota.py \
     /data/srv/monit

ENV PYTHONPATH=/data/srv/current/lib/python/site-packages:${PYTHONPATH:-}

ENV RUCIO_HOME=/cvmfs/cms.cern.ch/rucio/current/
ENV RUCIO_ACCOUNT="crab_server"
# no need to source a rucio environment from cvmfs. rucio client is already installed
# via pip, do not pollute the python env with things you do not need!
# ENV PYTHONPATH=$PYTHONPATH:/cvmfs/cms.cern.ch/rucio/x86_64/el9/py3/current/lib/python3.9/site-packages/
# ENV PYTHONPATH=$PYTHONPATH:/cvmfs/cms.cern.ch/rucio/x86_64/el8/py3/current/lib/python3.6/site-packages/

ENTRYPOINT ["tini", "--"]

CMD bash
# CMD python3 /data/srv/monit/GenerateMONIT.py
# CMD python3.8 /data/srv/monit/CheckTapeRecall.py
    # cp -v RecallRules.html /data/eos/RecallRules-docker.html
    ## we decided to send data to opensearch instead of saving it to eos. no need to copy the file.
