# Get CERN tsanames.ora, process_exporter from cmsweb rpm image
FROM registry.cern.ch/cmsweb/cmsweb:20230427-stable AS cmsweb
FROM registry.cern.ch/cmsweb/exporters:20250519-stable AS exporters

# Use `wmagent-base` image as our baseimage:
# https://github.com/dmwm/CMSKubernetes/blob/2b0454f9205cb8f97fecb91bf6661b59e4b31424/docker/pypi/wmagent-base/Dockerfile

FROM registry.cern.ch/cmsweb/wmagent-base:pypi-20230705
FROM registry.cern.ch/cmsweb/oracle:23_9-go1.25-oci8-deb13-aio-stable AS oracle
####### Expected to be replaced derived image #######
# FROM registry.cern.ch/cmsweb/dmwm-base:alma9-py3.12-20250728-stable
####### Start Expected Nested Image Patch #######
FROM cern/alma9-base:latest

# Required OS packages
RUN dnf -y upgrade && \
    dnf -y install --skip-broken curl libcurl && \
    dnf -y install sudo vim less procps && \
    dnf -y install python3.12 python3.12-pip python3-pycurl pip && \
    dnf clean all
RUN ln -s /usr/bin/python3.12 /usr/bin/python
RUN ln -sf /usr/bin/pip3.12 /usr/bin/pip

ENV WDIR=/data
ADD cicd/crabtaskworker_pypi/run.sh $WDIR/run.sh
ADD cicd/crabserver_pypi/monitor.sh $WDIR/monitor.sh
ADD cicd/crabtaskworker_pypi/manage $WDIR/manage
WORKDIR /data
####### END Expected Nested Image Patch #######

RUN dnf install -y epel-release # required by rlwrap package.
RUN dnf -y update && \
    dnf -y install \
      # basic OS packages
      cronie cronie-anacron \
      mariadb-server mariadb-connector-c-devel \
      myproxy voms-clients \
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
# Renaming are not necessary in alma9, Since libaio live well in /usr/lib64/libaio.so.1
# RUN ln -s /usr/lib/x86_64-linux-gnu/libaio.so.1t64.0.2 /usr/lib/x86_64-linux-gnu/libaio.so.1
ENV LD_LIBRARY_PATH=/usr/lib/oracle
ENV PATH=$PATH:/usr/lib/oracle
ENV PKG_CONFIG_PATH=/usr/lib/oracle

SHELL ["/bin/bash", "-c"]
ENV WDIR=/data
ENV USER=crab3
RUN useradd -m ${USER} \
    && install -o ${USER} -d ${WDIR} \
    && echo "${USER} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/01-crab3

# install packages from debian official repo
RUN apt-get update \
    && apt-get install -y tini git bash-completion fd-find ripgrep \
    && apt-get clean all

# local timezone (hardcode)
RUN ln -sf /usr/share/zoneinfo/Europe/Zurich /etc/localtime

# Copy CERN tnsnames.ora
COPY --from=cmsweb /etc/tnsnames.ora /etc/tnsnames.ora
# CMSWEB utilities: certificates, monitoring, exporters, etc
COPY --from=exporters /data/cmsweb-ping /usr/bin/cmsweb-ping
COPY --from=exporters /data/process_exporter /usr/bin/process_exporter
COPY --from=exporters /data/cpy_exporter /usr/bin/cpy_exporter
COPY --from=exporters /data/process_monitor.sh /usr/bin/process_monitor.sh

COPY --chown=${USER}:${USER} cicd/crabserver_pypi/monitor.sh ${WDIR}/monitor.sh

RUN mkdir /build
WORKDIR /build

COPY cicd/crabserver_pypi/requirements.txt .
# Install dependencies
RUN pip install -r requirements.txt

# create install dir
RUN install -d -o ${USER} -g ${USER} ${WDIR}/srv/current/lib/python/site-packages

# Install wmcore
COPY cicd/crabserver_pypi/wmcore_requirements.txt \
     cicd/crabserver_pypi/installWMCore.sh \
     ./
RUN ./installWMCore.sh wmcore_requirements.txt ${WDIR}/srv/current/lib/python/site-packages

# Install CRAB.
COPY --chown=${USER}:${USER} src/python/ ${WDIR}/srv/current/lib/python/site-packages
COPY --chown=${USER}:${USER} src/script/ ${WDIR}/srv/current/data/script
COPY --chown=${USER}:${USER} src/html/ ${WDIR}/srv/current/data/html
COPY --chown=${USER}:${USER} src/css/ ${WDIR}/srv/current/data/css

WORKDIR ${WDIR}

# cleanup build dir
RUN rm -rf /build

# add github repos
COPY --chown=${USER}:${USER} cicd/crabserver_pypi/addGH.sh .
USER ${USER}
RUN bash addGH.sh
USER root

# create mandatory directory
RUN ls ${WDIR}/srv/current/ \
    && install -d -o ${USER} -g ${USER} ${WDIR}/srv/logs/crabserver \
    && install -d -o ${USER} -g ${USER} ${WDIR}/srv/state/crabserver \
    && install -d -o ${USER} -g ${USER} ${WDIR}/srv/current/auth/crabserver \
    && install -d -o ${USER} -g ${USER} ${WDIR}/srv/current/config/crabserver

# remove unuse /data/manage come from baseimage to prevent confusion
RUN rm ${WDIR}/manage

# copy running script files
COPY --chown=${USER}:${USER} cicd/crabserver_pypi/run.sh \
     cicd/crabserver_pypi/manage.sh \
     cicd/crabserver_pypi/manage.py \
     cicd/crabserver_pypi/entrypoint.sh \
     cicd/crabserver_pypi/env.sh \
     cicd/crabserver_pypi/start.sh \
     cicd/crabserver_pypi/stop.sh \
     cicd/crabserver_pypi/status.sh \
     /data

USER ${USER}

CMD ${WDIR}/run.sh
