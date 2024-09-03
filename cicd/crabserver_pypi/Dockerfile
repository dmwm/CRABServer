# Get CERN tsanames.ora, process_exporter from cmsweb rpm image
FROM registry.cern.ch/cmsweb/cmsweb:20230427-stable AS cmsweb

# Use `wmagent-base` image as our baseimage:
# https://github.com/dmwm/CMSKubernetes/blob/2b0454f9205cb8f97fecb91bf6661b59e4b31424/docker/pypi/wmagent-base/Dockerfile

FROM registry.cern.ch/cmsweb/wmagent-base:pypi-20230705
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
# Copy process exporter
COPY --from=cmsweb ${WDIR}/cmsweb/bin/process_monitor.sh \
     ${WDIR}/cmsweb/bin/process_exporter \
     /usr/local/bin
COPY --chown=${USER}:${USER} cicd/crabserver_pypi/monitor.sh ${WDIR}/monitor.sh

RUN mkdir /build
WORKDIR /build

COPY cicd/crabserver_pypi/requirements.txt cicd/crabserver_pypi/wmcore_requirements.txt .
# Install wmcore/crab dependencies
RUN pip install -r requirements.txt
# Getting the version number from wmcore_requirements.txt and install it with `--no-deps`.
# When reading from file, `grep -v '^\s*#'` to ignore all comment lines, including the ones with leading whitespace ('\s' to match whitespace).
RUN wmcore_version="$(grep -v '^\s*#' wmcore_requirements.txt | cut -d' ' -f2)" \
    && pip install --no-deps WMCore==${wmcore_version}

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
     /data

USER ${USER}

CMD ${WDIR}/run.sh
