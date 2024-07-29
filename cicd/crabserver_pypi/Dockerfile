# Get CERN tsanames.ora, process_exporter from cmsweb rpm image
FROM registry.cern.ch/cmsweb/cmsweb:20230427-stable AS cmsweb

# Use `wmagent-base` image as our baseimage:
# https://github.com/dmwm/CMSKubernetes/blob/2b0454f9205cb8f97fecb91bf6661b59e4b31424/docker/pypi/wmagent-base/Dockerfile

FROM registry.cern.ch/cmsweb/wmagent-base:pypi-20230705
SHELL ["/bin/bash", "-c"]
ENV WDIR=/data
ENV USER=crab3
RUN useradd ${USER} \
    && install -o ${USER} -d ${WDIR} \
    && echo "${USER} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/01-crab3

# install packages from debian official repo
RUN apt-get update \
    && apt-get install -y tini \
    && apt-get clean all

# local timezone (hardcode)
RUN ln -sf /usr/share/zoneinfo/Europe/Zurich /etc/localtime

# Copy CERN tnsnames.ora
COPY --from=cmsweb /etc/tnsnames.ora /etc/tnsnames.ora
# Copy process exporter
COPY --from=cmsweb /data/cmsweb/bin/process_monitor.sh \
     /data/cmsweb/bin/process_exporter \
     /usr/local/bin
COPY cicd/crabserver_pypi/monitor.sh /data/monitor.sh

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
COPY src/python/ /data/srv/current/lib/python/site-packages
COPY src/script/ /data/srv/current/data/script/
COPY src/html/ /data/srv/current/data/html/
COPY src/css/ /data/srv/current/data/css/

# add github repos
COPY cicd/crabserver_pypi/addGH.sh .
RUN bash addGH.sh

RUN rm -rf /build

WORKDIR /data

# create mandatory directory
RUN ls /data/srv/current/ \
    && install -d -o ${USER} -g ${USER} /data/srv/logs/crabserver \
    && install -d -o ${USER} -g ${USER} /data/srv/state/crabserver \
    && install -d -o ${USER} -g ${USER} /data/srv/current/auth/crabserver \
    && install -d -o ${USER} -g ${USER} /data/srv/current/config/crabserver

# remove unuse /data/manage come from baseimage to prevent confusion
RUN rm /data/manage

# copy running script files
COPY cicd/crabserver_pypi/run.sh \
     cicd/crabserver_pypi/manage.sh \
     cicd/crabserver_pypi/entrypoint.sh \
     cicd/crabserver_pypi/env.sh \
     cicd/crabserver_pypi/start.sh \
     cicd/crabserver_pypi/stop.sh \
     /data/

# make sure all files is the same user/group as running user
RUN chown -R 1000:1000 /data && chmod +x /data/*.sh

USER ${USER}
CMD /data/run.sh
