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

# gfal, wmcore only has gfal lib but not cli
FROM registry.cern.ch/cmsweb/gfal:latest AS wmcore-gfal
SHELL ["/bin/bash", "-c"]
RUN pushd /data/miniconda \
    && git clone https://gitlab.cern.ch/dmc/gfal2-util -b v1.8.0 \
    && pushd gfal2-util \
    && python setup.py install \
    && popd \
    && rm -rf gfal2-util

# cern ldap config
FROM gitlab-registry.cern.ch/linuxsupport/cc7-base:20231001-1.x86_64 AS cern-cc7

# start image
####### Expected to be replaced derived image #######
# FROM registry.cern.ch/cmsweb/wmagent-base:pypi-20230705
####### Expected Patch #######
FROM registry.cern.ch/cmsweb/oracle:23_9-go1.25-oci8-deb13-aio-stable as oracle
FROM registry.cern.ch/cmsweb/dmwm-base:alma9-py3.9-20250703

RUN dnf -y update && \
    dnf -y install \
      # basic OS packages
      cronie cronie-anacron \
      mariadb-server mariadb-connector-c-devel \
      myproxy \
      # voms-clients, voms-clients-java included in cern/alma9
      rlwrap libaio \
      # build-essential
      gcc gcc-c++ make glibc-devel \
      # required for yui
      wget unzip \
      # debugging
      hostname net-tools iputils procps-ng jq \
      # Install recursive ps utility tool (pstree, a replacement of pslist)
      psmisc \
      s-nail && \
    dnf -y clean all && \
    rm -rf /var/cache/dnf

# copy oracle client:
COPY --from=oracle /usr/lib/oracle /usr/lib/oracle
# Renaming are not necessary in alma9, unlike debian which introduced t64 suffix for ABI split.
# RUN ln -s /usr/lib/x86_64-linux-gnu/libaio.so.1t64.0.2 /usr/lib/x86_64-linux-gnu/libaio.so.1
ENV LD_LIBRARY_PATH=/usr/lib/oracle
ENV PATH=$PATH:/usr/lib/oracle
ENV PKG_CONFIG_PATH=/usr/lib/oracle
####### End Expected Patch #######

SHELL ["/bin/bash", "-c"]
ENV USER=crab3
ENV WDIR=/data

# add new user
RUN useradd -m ${USER} \
    && install -o ${USER} -d ${WDIR}

# install gfal
# symlink to workaround calling gfal from absolute path
COPY --chown=${USER}:${USER} --from=wmcore-gfal ${WDIR}/miniconda ${WDIR}/miniconda/
RUN ln -sf ${WDIR}/miniconda/bin/gfal-ls /usr/bin/gfal-ls \
    && ln -sf ${WDIR}/miniconda/bin/gfal-rm /usr/bin/gfal-rm \
    && ln -sf ${WDIR}/miniconda/bin/gfal-copy /usr/bin/gfal-copy \
    && ln -sf ${WDIR}/miniconda/bin/gfal-sum /usr/bin/gfal-sum

# install package from debian repository
# deps for openldap: libsasl2-dev python3-dev libldap-dev libssl-dev
RUN apt-get update \
    && apt-get install -y tini voms-clients-java libsasl2-dev python3-dev libldap-dev libssl-dev zip git fd-find ripgrep bash-completion \
    && apt-get clean all

# local timezone (hardcode)
RUN ln -sf /usr/share/zoneinfo/Europe/Zurich /etc/localtime

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

# copy cern openldap config
COPY --chown=${USER}:${USER} --from=cern-cc7 /etc/openldap /etc/openldap/

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
COPY --chown=${USER}:${USER} cicd/crabtaskworker_pypi/run.sh /data/

# for debuggin purpose
RUN echo "${USER} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/01-crab3

USER ${USER}

ENTRYPOINT ["tini", "--"]
CMD ["/data/run.sh"]
