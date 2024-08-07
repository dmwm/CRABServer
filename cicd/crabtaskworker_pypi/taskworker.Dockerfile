ARG BASE_TAG=latest
FROM registry.cern.ch/cmscrab/crabtaskworker:${BASE_TAG}.base

# caching wmcore src, need for building TaskManagerRun.tar.gz
FROM python:3.8 as wmcore-src
SHELL ["/bin/bash", "-c"]
# Use the "magic" requirements.txt from crabserver pypi
COPY cicd/crabserver_pypi/ .
RUN wmcore_repo="$(grep -v '^\s*#' wmcore_requirements.txt | cut -d' ' -f1)" \
    && wmcore_version="$(grep -v '^\s*#' wmcore_requirements.txt | cut -d' ' -f2)" \
    && git clone ${wmcore_repo} -b "${wmcore_version}" /WMCore \
    && ( cd /WMCore; git status ) \
    && echo "${wmcore_version}" > /wmcore_version

# create data files ./data
FROM python:3.8 as build-data
SHELL ["/bin/bash", "-c"]
RUN mkdir /build \
    && apt-get update \
    && apt-get install -y curl zip git \
    && apt-get clean all
WORKDIR /build
COPY cicd/crabtaskworker_pypi/buildTWTarballs.sh cicd/crabtaskworker_pypi/buildDatafiles.sh /build/
COPY setup.py /build
COPY src /build/src
COPY scripts /build/scripts
COPY --from=wmcore-src /WMCore /build/WMCore
RUN WMCOREDIR=./WMCore \
    CRABSERVERDIR=./ \
    DATAFILES_WORKDIR=./data_files\
    bash buildDatafiles.sh

# start image
FROM registry.cern.ch/cmscrab/crabtaskworker:${BASE_TAG}.base as base-image

# copy TaskManagerRun.tar.gz
COPY --from=build-data /build/data_files/data ${WDIR}/srv/current/lib/python/site-packages/data

# install crabserver
# will replace with pip later
COPY src/python/ ${WDIR}/srv/current/lib/python/site-packages/

# copy process executor scripts
## TaskWorker
COPY cicd/crabtaskworker_pypi/TaskWorker/start.sh \
     cicd/crabtaskworker_pypi/TaskWorker/env.sh \
     cicd/crabtaskworker_pypi/TaskWorker/stop.sh \
     cicd/crabtaskworker_pypi/TaskWorker/manage.sh \
     cicd/crabtaskworker_pypi/updateDatafiles.sh \
     ${WDIR}/srv/TaskManager/

COPY cicd/crabtaskworker_pypi/bin/crab-taskworker /usr/local/bin/crab-taskworker

## publisher
COPY cicd/crabtaskworker_pypi/Publisher/start.sh \
     cicd/crabtaskworker_pypi/Publisher/env.sh \
     cicd/crabtaskworker_pypi/Publisher/stop.sh \
     cicd/crabtaskworker_pypi/Publisher/manage.sh \
     ${WDIR}/srv/Publisher/

## entrypoint
COPY cicd/crabtaskworker_pypi/run.sh /data
USER root

# for debugging purpose
RUN echo "${USER} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/01-crab3

# ensure all /data owned by running user
# RUN chown -R 1000:1000 ${WDIR}

USER ${USER}

ENTRYPOINT ["tini", "--"]
CMD ["/data/run.sh"]
