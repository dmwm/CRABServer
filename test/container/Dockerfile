FROM cmssw/cmsweb:20210315

RUN yum install -y gfal2-util gfal2-all uberftp python3

RUN mkdir -p /data/CRABTesting
RUN chmod 777 /data/CRABTesting/
ENV WDIR=/data/CRABTesting
WORKDIR ${WDIR}

COPY testingScripts ${WDIR}/testingScripts
RUN chmod 777 ${WDIR}/testingScripts
WORKDIR ${WDIR}/testingScripts

ENTRYPOINT ["/bin/bash"]
