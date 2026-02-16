# create data files ./data
FROM python:3.8 AS build-data
SHELL ["/bin/bash", "-c"]
RUN mkdir /build \
    && apt-get update \
    && apt-get install -y curl zip git \
    && apt-get clean all
WORKDIR /build
COPY cicd/crabtaskworker_pypi/buildTWTarballs.sh \
     cicd/crabtaskworker_pypi/buildDatafiles.sh \
     cicd/crabserver_pypi/wmcore_requirements.txt /build/
COPY setup.py /build
COPY src /build/src
COPY scripts /build/scripts

RUN wmcore_repo="$(grep -v '^\s*#' wmcore_requirements.txt | cut -d' ' -f1)" \
    && wmcore_version="$(grep -v '^\s*#' wmcore_requirements.txt | cut -d' ' -f2)" \
    && git clone  --depth 1 ${wmcore_repo} -b "${wmcore_version}" /build/WMCore \
    && ( cd /build/WMCore; git status )
RUN WMCOREDIR=./WMCore \
    CRABSERVERDIR=./ \
    DATAFILES_WORKDIR=./data_files\
    bash buildDatafiles.sh

### Start Run-time environment ###
# cern ldap config
FROM registry.cern.ch/cmsweb/exporters:20250519-stable AS exporters
FROM cern/alma9-base:latest

ENV USER=crab3
ENV WDIR=/data
WORKDIR /data

# Add CERN Everything EPEL 9 repo with GPG_KEY.
ADD cicd/crabtaskworker_pypi/cern-epel9.repo /etc/yum.repos.d/cern-epel9.repo
ADD cicd/crabtaskworker_pypi/RPM-GPG-KEY-EPEL-9 /etc/pki/rpm-gpg/RPM-GPG-KEY-EPEL-9

# Import GPG_KEY
RUN rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-EPEL-9

RUN dnf -y update && \
    dnf -y install --skip-broken curl libcurl && \
    dnf -y install \
      # basic OS packages \
      sudo vim less procps \
      tini git \
      zip \
      fd-find ripgrep bash-completion \
      wget unzip \
      # debugging & recursive ps utility tool (pstree, a replacement of pslist) \
      hostname net-tools iputils procps-ng jq \
      psmisc s-nail \
      # base OS packages \
      cronie cronie-anacron \
      mariadb-server mariadb-connector-c-devel \
      rlwrap \
      # build-essential \
      gcc gcc-c++ make glibc-devel \
      # CRAB & WLCG specfic deps & Python deps in Alma9 \
      pip \
      python3-pycurl \
      python3-devel \
      python3-setuptools \
      python3-wheel \
      libcurl-devel \
      libaio-devel \
      libaio \
      myproxy \
      voms-clients \
      # gfal via RPM \
      gfal2 \
      gfal2-util-scripts \
      gfal2-plugin-file \
      gfal2-plugin-http \
      gfal2-plugin-xrootd \
   && dnf clean all \
   && rm -rf /var/cache/dnf

# enforce python version
RUN ln -s /usr/bin/python3.9 /usr/bin/python
# local timezone (hardcode)
RUN ln -sf /usr/share/zoneinfo/Europe/Zurich /etc/localtime

# add CRAB user
RUN useradd -m ${USER} \
    && install -o ${USER} -d ${WDIR}

# CMSWEB utilities: certificates, monitoring, exporters, etc
COPY --from=exporters /data/cmsweb-ping /usr/bin/cmsweb-ping
COPY --from=exporters /data/process_exporter /usr/bin/process_exporter
COPY --from=exporters /data/cpy_exporter /usr/bin/cpy_exporter
COPY --from=exporters /data/process_monitor.sh /usr/bin/process_monitor.sh

# prepare build
RUN mkdir /build
WORKDIR /build

# install dependencies
COPY cicd/crabtaskworker_pypi/requirements.txt ./
RUN pip install -r requirements.txt \
    && pip cache purge

# create install dir
RUN install -d -o ${USER} -g ${USER} ${WDIR}/srv/current/lib/python/site-packages

# install wmcore
COPY cicd/crabserver_pypi/wmcore_requirements.txt \
     cicd/crabserver_pypi/installWMCore.sh \
     ./
RUN ./installWMCore.sh wmcore_requirements.txt ${WDIR}/srv/current/lib/python/site-packages

# install crabserver
# will replace with pip later
COPY --chown=${USER}:${USER} src/python/ ${WDIR}/srv/current/lib/python/site-packages

# copy TaskManagerRun.tar.gz
COPY --chown=${USER}:${USER} --from=build-data /build/data_files/data ${WDIR}/srv/current/lib/python/site-packages/data/

# copy htcondor config
RUN mkdir /etc/condor
COPY --chown=${USER}:${USER} cicd/crabtaskworker_pypi/condor_config /etc/condor/

# copy rucio config
RUN mkdir -p /opt/rucio/etc/
COPY --chown=${USER}:${USER} cicd/crabtaskworker_pypi/rucio.cfg /opt/rucio/etc/

# add github repos, reuse script in crabserver_pypi
COPY --chown=${USER}:${USER} cicd/crabserver_pypi/addGH.sh ./
USER ${USER}
RUN bash addGH.sh
USER root

# clean up
WORKDIR ${WDIR}
RUN rm -rf /build

# create working directory
RUN install -d -o ${USER} -g ${USER} ${WDIR}/srv/tmp
# Create directories for TaskWorker
RUN install -d -o ${USER} -g ${USER} ${WDIR}/srv/TaskManager/current \
    && install -d -o ${USER} -g ${USER} ${WDIR}/srv/TaskManager/cfg \
    && install -d -o ${USER} -g ${USER} ${WDIR}/srv/TaskManager/logs \
    # Change ownership to the running user
    && chown -R ${USER}:${USER} ${WDIR}/srv

# Create directories for Publisher
RUN install -d -o ${USER} -g ${USER} ${WDIR}/srv/Publisher/current \
    && install -d -o ${USER} -g ${USER} ${WDIR}/srv/Publisher/cfg \
    && install -d -o ${USER} -g ${USER} ${WDIR}/srv/Publisher/logs \
    && install -d -o ${USER} -g ${USER} ${WDIR}/srv/Publisher/PublisherFiles \
    # Change ownership of parent and current directories to the running user
    && chown -R ${USER}:${USER} ${WDIR}/srv

# copy process executor scripts
## TaskWorker
COPY --chown=${USER}:${USER} cicd/crabtaskworker_pypi/TaskWorker/manage.sh \
     cicd/crabtaskworker_pypi/updateDatafiles.sh \
     ${WDIR}/srv/TaskManager/
## Publisher
COPY --chown=${USER}:${USER} cicd/crabtaskworker_pypi/Publisher/manage.sh \
     ${WDIR}/srv/Publisher/

## common script
COPY --chown=${USER}:${USER} cicd/crabserver_pypi/start.sh \
     cicd/crabserver_pypi/env.sh \
     cicd/crabserver_pypi/stop.sh \
     cicd/crabserver_pypi/status.sh \
     cicd/crabserver_pypi/manage.py \
     ${WDIR}/srv/TaskManager/

COPY --chown=${USER}:${USER} cicd/crabserver_pypi/start.sh \
     cicd/crabserver_pypi/env.sh \
     cicd/crabserver_pypi/stop.sh \
     cicd/crabserver_pypi/status.sh \
     cicd/crabserver_pypi/manage.py \
     ${WDIR}/srv/Publisher/

## binary
COPY --chown=${USER}:${USER} cicd/crabtaskworker_pypi/bin/crab-taskworker cicd/crabtaskworker_pypi/bin/crab-publisher /usr/local/bin/

## entrypoint
COPY --chown=${USER}:${USER} cicd/crabtaskworker_pypi/run.sh \
  cicd/crabserver_pypi/monitor.sh \
  $WDIR

# for debuggin purpose
RUN echo "${USER} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/01-crab3

USER ${USER}

ENTRYPOINT ["tini", "--"]
CMD ["/data/run.sh"]
