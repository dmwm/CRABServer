FROM registry.cern.ch/cmsweb/cmsweb:20220301-stable
MAINTAINER Daina Dirmaite daina.dirmaite@gmail.com

#keep only voms-proxy basded on c++
RUN yum remove -y voms-clients-java && yum -y install strace \
gfal2-util gfal2-all \
vim \
&& yum clean all && rm -rf /var/cache/yum

ENV WDIR=/data
ENV USER=crab3

# add new user and switch to user
RUN useradd ${USER} && install -o ${USER} -d ${WDIR}
USER ${USER}

ARG RELEASE_TAG
ENV RELEASE_TAG $RELEASE_TAG
ARG RPM_RELEASETAG_HASH
ENV RPM_RELEASETAG_HASH $RPM_RELEASETAG_HASH

RUN mkdir -p /data/srv/tmp && mkdir -p /data/srv/Publisher && mkdir -p /data/srv/TaskManager
WORKDIR ${WDIR}

# install
COPY --chown=${USER}:${USER} install.sh ./
RUN echo "Y" > yes.repo && sh install.sh < yes.repo && rm yes.repo

# add needed script (in a different line to avoid full rpm install when they change)
COPY --chown=${USER}:${USER} addGH.sh monitor.sh run.sh ./
# add GitHub repositories
RUN ./addGH.sh

# run the service
CMD sh /data/run.sh && sh /data/monitor.sh && while true; do sleep 60;done
