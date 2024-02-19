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

# create TaskManagerRun.tar.gz and other data files
FROM python:3.8 as build-data
SHELL ["/bin/bash", "-c"]
RUN mkdir /build \
    && apt-get update \
    && apt-get install -y curl zip git \
    && apt-get clean all
WORKDIR /build
COPY cicd/crabtaskworker_pypi/new_htcondor_make_runtime.sh cicd/crabtaskworker_pypi/build_data_files.sh /build/
COPY setup.py /build
COPY src /build/src
COPY scripts /build/scripts
COPY --from=wmcore-src /WMCore /build/WMCore
RUN WMCOREDIR=./WMCore CRABSERVERDIR=. bash build_data_files.sh

# gfal, wmcore only has gfal lib but not cli
FROM registry.cern.ch/cmsweb/gfal:latest as wmcore-gfal
SHELL ["/bin/bash", "-c"]
RUN pushd /data/miniconda \
    && git clone https://gitlab.cern.ch/dmc/gfal2-util -b v1.8.0 \
    && pushd gfal2-util \
    && python setup.py install \
    && popd \
    && rm -rf gfal2-util

# cern ldap config
FROM gitlab-registry.cern.ch/linuxsupport/cc7-base:20231001-1.x86_64 as cern-cc7

# start image
FROM registry.cern.ch/cmsweb/wmagent-base:pypi-20230705
SHELL ["/bin/bash", "-c"]
ENV USER=crab3
ENV WDIR=/data

# install gfal
# symlink to workaround calling gfal from absolute path
COPY --from=wmcore-gfal ${WDIR}/miniconda ${WDIR}/miniconda
RUN ln -sf ${WDIR}/miniconda/bin/gfal-ls /usr/bin/gfal-ls \
    && ln -sf ${WDIR}/miniconda/bin/gfal-rm /usr/bin/gfal-rm \
    && ln -sf ${WDIR}/miniconda/bin/gfal-copy /usr/bin/gfal-copy \
    && ln -sf ${WDIR}/miniconda/bin/gfal-sum /usr/bin/gfal-sum

# install package from debian repository
# deps for openldap: libsasl2-dev python3-dev libldap-dev libssl-dev
RUN apt-get update \
    && apt-get install -y tini git zip fd-find ripgrep libsasl2-dev python3-dev libldap-dev libssl-dev \
    && apt-get clean all

# prepare build
RUN mkdir /build
WORKDIR /build

# install dependencies
COPY --from=wmcore-src /wmcore_version .
COPY cicd/crabtaskworker_pypi/requirements.txt .
RUN pip install -r requirements.txt \
    && pip install --no-deps wmcore==$(cat wmcore_version) \
    && pip cache purge

# install crabserver
# will replace with pip later
COPY src/python/ ${WDIR}/srv/current/lib/python/site-packages/
# copy TaskManagerRun.tar.gz
COPY --from=build-data /build/install_dir/data ${WDIR}/srv/current/lib/python/site-packages/data

# copy cern openldap config
COPY --from=cern-cc7 /etc/openldap /etc/openldap

# copy rucio config
RUN mkdir -p /opt/rucio/etc/
COPY cicd/crabtaskworker_pypi/rucio.cfg /opt/rucio/etc/

# add github repos, reuse script in crabserver_pypi
COPY cicd/crabserver_pypi/addGH.sh .
RUN bash addGH.sh

# clean up
WORKDIR ${WDIR}
RUN rm -rf /build

# add new user and switch to user
RUN useradd -m ${USER} \
    && install -o ${USER} -d ${WDIR}

# create working directory
RUN mkdir -p ${WDIR}/srv/tmp \
    && mkdir -p ${WDIR}/srv/Publisher \
    && mkdir -p ${WDIR}/srv/TaskManager

RUN cd ${WDIR}/srv/TaskManager \
    && ln -sf ${WDIR}/hostdisk/TaskWorker/cfg cfg \
    && ln -sf ${WDIR}/hostdisk/TaskWorker/logs logs \
    && rm /data/manage /data/run.sh

COPY cicd/crabtaskworker_pypi/start.sh cicd/crabtaskworker_pypi/stop.sh cicd/crabtaskworker_pypi/run.sh cicd/crabtaskworker_pypi/manage cicd/crabtaskworker_pypi/new_updateTMRuntime.sh  ${WDIR}/srv/TaskManager/

# for debuggin purpose
RUN echo "${USER} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/01-crab3

# make sure all /data own by running user
RUN chown -R 1000:1000 ${WDIR}

USER ${USER}

ENTRYPOINT ["tini", "--"]
