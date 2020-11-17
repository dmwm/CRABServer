FROM cmssw/cmsweb:20200901
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

ARG TW_VERSION
ENV RELEASE $TW_VERSION
ENV TW_VERSION $TW_VERSION

RUN mkdir -p /data/srv/tmp && mkdir -p /data/srv/Publisher && mkdir -p /data/srv/TaskManager
WORKDIR ${WDIR}

# install
COPY --chown=${USER}:${USER} install.sh monitor.sh ./
RUN echo "Y" > yes.repo && sh install.sh < yes.repo

RUN echo '"cms" "lcg-voms2.cern.ch" "15002" "/DC=ch/DC=cern/OU=computers/CN=lcg-voms2.cern.ch" "cms" "24"' > /etc/vomses/cms-lcg-voms2.cern.ch && echo '"cms" "voms2.cern.ch" "15002" "/DC=ch/DC=cern/OU=computers/CN=voms2.cern.ch" "cms" "24"' > /etc/vomses/cms-voms2.cern.ch

# run the service
CMD sh start.sh && sh /data/monitor.sh && while true; do sleep 60;done
