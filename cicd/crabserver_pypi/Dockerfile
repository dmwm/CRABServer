# Get CERN tsanames.ora, process_exporter from cmsweb rpm image
FROM registry.cern.ch/cmsweb/cmsweb:wm-freeze AS cmsweb
FROM registry.cern.ch/cmsweb/exporters:20250519-stable AS exporters

FROM cern/alma9-base:latest

# Add Oracle OL9 key, repo, importing key
ADD https://linuxsoft.cern.ch/mirror/yum.oracle.com/RPM-GPG-KEY-oracle-ol9 /etc/pki/rpm-gpg/RPM-GPG-KEY-oracle-ol9
ADD cicd/crabserver_pypi/oracle-instantclient-ol9.repo /etc/yum.repos.d/oracle-instantclient-ol9.repo
RUN rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-oracle-ol9

# Add CERN Everything EPEL 9 repo with GPG_KEY.
ADD cicd/crabtaskworker_pypi/cern-epel9.repo /etc/yum.repos.d/cern-epel9.repo
ADD cicd/crabtaskworker_pypi/RPM-GPG-KEY-EPEL-9 /etc/pki/rpm-gpg/RPM-GPG-KEY-EPEL-9

ENV WDIR=/data
ENV USER=crab3

RUN dnf -y update && \
    dnf -y install --skip-broken curl libcurl && \
    dnf -y install \
      # basic OS packages \
      sudo vim less procps \
      tini git \
      fd-find ripgrep bash-completion \
      wget unzip \
      # debugging & recursive ps utility tool (pstree, a replacement of pslist) \
      hostname net-tools iputils procps-ng jq \
      psmisc s-nail \
      # base OS packages \
      cronie cronie-anacron \
      mariadb-server mariadb-connector-c-devel \
      rlwrap \
      # CERN Oracle client packages in Alma9 \
      oracle-instantclient19.29-basic \
      oracle-instantclient19.29-sqlplus \
      oracle-instantclient19.29-devel \
      oracle-instantclient19.29-tools \
      # build-essential \
      gcc gcc-c++ make glibc-devel \
      # CRAB & WLCG specfic deps & Python deps in Alma9 \
      pip \
      python3-pycurl \
      python3-devel \
      python3-setuptools \
      libcurl-devel \
      openssl-devel \
      libaio-devel \
      libaio \
      myproxy \
      voms-clients \
   && dnf clean all \
   && rm -rf /var/cache/dnf

# local timezone (hardcode)
RUN ln -sf /usr/share/zoneinfo/Europe/Zurich /etc/localtime
# Add CRAB User
RUN useradd -m ${USER} \
    && install -o ${USER} -d ${WDIR} \
    && echo "${USER} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/01-crab3


# Copy CERN tnsnames.ora
COPY --from=cmsweb /etc/tnsnames.ora /etc/tnsnames.ora
# CMSWEB utilities: certificates, monitoring, exporters, etc
COPY --from=exporters /data/cmsweb-ping /usr/bin/cmsweb-ping
COPY --from=exporters /data/process_exporter /usr/bin/process_exporter
COPY --from=exporters /data/cpy_exporter /usr/bin/cpy_exporter
COPY --from=exporters /data/process_monitor.sh /usr/bin/process_monitor.sh

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

# copy running script files
COPY --chown=${USER}:${USER} cicd/crabserver_pypi/run.sh \
     cicd/crabserver_pypi/manage.sh \
     cicd/crabserver_pypi/manage.py \
     cicd/crabserver_pypi/entrypoint.sh \
     cicd/crabserver_pypi/env.sh \
     cicd/crabserver_pypi/start.sh \
     cicd/crabserver_pypi/stop.sh \
     cicd/crabserver_pypi/status.sh \
     cicd/crabserver_pypi/monitor.sh \
     ${WDIR}

USER ${USER}

CMD ${WDIR}/run.sh
