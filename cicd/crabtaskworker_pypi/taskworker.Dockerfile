# start image
FROM registry.cern.ch/cmsweb/wmagent-base:pypi-20230705
SHELL ["/bin/bash", "-c"]
ENV USER=crab3
ENV WDIR=/data

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

# for debuggin purpose
RUN echo "${USER} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/01-crab3

USER ${USER}

ENTRYPOINT ["tini", "--"]
CMD ["/data/run.sh"]
