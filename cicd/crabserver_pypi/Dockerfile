# Get CERN tsanames.ora from cmsweb rpm image
FROM registry.cern.ch/cmsweb/cmsweb:20230427-stable as tnsnames

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
RUN apt-get update && apt-get install -y tini && apt-get clean all

# Copy CERN tnsnames.ora
COPY --from=tnsnames /etc/tnsnames.ora /etc/tnsnames.ora

#RUN mkdir -p /opt/rucio/etc/

RUN mkdir /build
WORKDIR /build
COPY cicd/crabserver_pypi/ .
# Getting wmcore version from wmcore_requirements.txt and install with `pip install --no-deps`
RUN pip install -r requirements.txt
RUN req=($(./requirementsParse.py wmcore_requirements.txt)) \
    && pip install --no-deps WMCore==${req[1]}

# Install CRAB.
COPY src/python/ /data/srv/current/lib/python3.8/site-packages
COPY src/script/ /data/srv/current/data/script/
COPY src/html/ /data/srv/current/data/html/
COPY src/css/ /data/srv/current/data/css/

RUN rm -rf /build

WORKDIR /data

# create mandatory directory
RUN ls /data/srv/current/ \
    && install -d -o ${USER} -g ${USER} /data/srv/logs/crabserver \
    && install -d -o ${USER} -g ${USER} /data/srv/state/crabserver \
    && install -d -o ${USER} -g ${USER} /data/srv/current/auth/crabserver \
    && install -d -o ${USER} -g ${USER} /data/srv/current/config/crabserver

# copy running script files
COPY cicd/crabserver_pypi/run.sh cicd/crabserver_pypi/manage cicd/crabserver_pypi/entrypoint.sh /data/

# make sure all files is the same user/group as running user
RUN chown -R 1000:1000 /data

USER ${USER}
CMD /data/run.sh
