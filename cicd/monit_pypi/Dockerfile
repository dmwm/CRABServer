ARG BASE_TAG=latest
FROM registry.cern.ch/cmscrab/crabtaskworker:${BASE_TAG}

# RUN apt update && apt install -y \
#     vim nano \
#     && \
#     apt clean && rm -rf /var/lib/apt/lists/*
# # the following packages are necessary, but already provided inside the docker image
# #  libcurl-openssl-dev, libssl-dev \

# ENV WDIR=/data
# ENV USER=crab3
# # add new user and switch to user
# RUN useradd ${USER} &&\
#     install -o ${USER} -d ${WDIR} &&\
#     install -o ${USER} -d /home/${USER}
# USER ${USER}

# pip is already available in the python:3.8 docker image
# RUN python3.8 -m ensurepip
RUN python3.8 -m pip install --user pandas 
# not necessary: rucio-clients pycurl jwt future

RUN mkdir -p /data/srv/tmp
# WORKDIR ${WDIR}

# RUN git clone https://github.com/dmwm/WMCore /data/srv/monit/WMCore
RUN mkdir -p /data/srv/monit/
COPY ./src/python/ServerUtilities.py           /data/srv/monit
COPY ./src/python/RESTInteractions.py          /data/srv/monit
COPY ./src/script/Monitor/GenerateMONIT.py     /data/srv/monit
COPY ./scripts/Utils/CheckTapeRecall.py        /data/srv/monit
COPY ./src/script/Monitor/aso_metrics_ora.py   /data/srv/monit
COPY ./src/script/Monitor/ReportRecallQuota.py /data/srv/monit

ENV PYTHONPATH=/data/srv/current/lib/python/site-packages:${PYTHONPATH:-}

ENV RUCIO_HOME=/cvmfs/cms.cern.ch/rucio/current/
ENV RUCIO_ACCOUNT="crab_server"
# no need to source a rucio environment from cvmfs. rucio client is already installed
# via pip, do not pollute the python env with things you do not need!
# ENV PYTHONPATH=$PYTHONPATH:/cvmfs/cms.cern.ch/rucio/x86_64/el9/py3/current/lib/python3.9/site-packages/
# ENV PYTHONPATH=$PYTHONPATH:/cvmfs/cms.cern.ch/rucio/x86_64/el8/py3/current/lib/python3.6/site-packages/

COPY ./cicd/build/monit-python/entrypoint.sh /entrypoint.sh
ENTRYPOINT [ "/bin/bash", "/entrypoint.sh" ]

CMD python3 /data/srv/monit/GenerateMONIT.py
# CMD python3.8 /data/srv/monit/CheckTapeRecall.py
    # cp -v RecallRules.html /data/eos/RecallRules-docker.html
    ## we decided to send data to opensearch instead of saving it to eos. no need to copy the file.
