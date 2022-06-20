FROM registry.cern.ch/cmsweb/cmsweb:20220601-stable

RUN yum install -y gfal2-util gfal2-all uberftp python3

RUN mkdir -p /data/CRABTesting
RUN chmod 777 /data/CRABTesting/
ENV WORK_DIR=/data/CRABTesting
WORKDIR ${WORK_DIR}

COPY testingScripts ${WORK_DIR}/testingScripts
RUN chmod 777 ${WORK_DIR}/testingScripts
WORKDIR ${WORK_DIR}/testingScripts

ENTRYPOINT ["/bin/bash", "-l", "-c"]
CMD ["/bin/bash"]
