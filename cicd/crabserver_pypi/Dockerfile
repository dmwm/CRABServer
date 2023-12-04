# Weird enough that we cannot use tnsnames from koji build
#FROM almalinux:8 as tnsnames
#
#RUN curl -Lo tnsnames.rpm https://koji.cern.ch/kojifiles/packages/oracle-instantclient-tnsnames.ora/1.4.6/1.rh8.cern/noarch/oracle-instantclient-tnsnames.ora-1.4.6-1.rh8.cern.noarch.rpm \
#    && rpm2archive tnsnames.rpm \
#    && tar -zxvf tnsnames.rpm.tgz  --strip-components=1 \
#    && ls -ahl
# Get it from cmsweb images instead.
FROM registry.cern.ch/cmsweb/cmsweb:20230427-stable as tnsnames

FROM registry.cern.ch/cmsweb/wmagent-base:pypi-20230705
SHELL ["/bin/bash", "-c"]
ENV WDIR=/data
ENV USER=crab3

# install package from debian official repo
RUN apt-get update && apt-get install -y tini && apt-get clean all
RUN mkdir /build
WORKDIR /build
COPY cicd/crabserver_pypi/ .
RUN pip install -r requirements.txt
RUN req=($(./requirementsParse.py)) \
    && pip install --no-deps WMCore==${req[1]}
RUN useradd ${USER} \
    && install -o ${USER} -d ${WDIR} \
    && mkdir -p /opt/rucio/etc/

# install cern tnsnames.ora
COPY --from=tnsnames /etc/tnsnames.ora /etc/tnsnames.ora

# Install CRAB, will convert to pypi later
# Will convert to pip install step later
COPY src/python/ /data/srv/current/lib/python3.8/site-packages
COPY src/script/ /data/srv/current/data/script/
COPY src/html/ /data/srv/current/data/html/
COPY src/css/ /data/srv/current/data/css/

WORKDIR /data
RUN rm -rf /build
RUN ls /data/srv/current/ \
    && install -d -o ${USER} -g ${USER} /data/srv/logs/crabserver \
    && install -d -o ${USER} -g ${USER} /data/srv/state/crabserver \
    && install -d -o ${USER} -g ${USER} /data/srv/current/auth/crabserver \
    && install -d -o ${USER} -g ${USER} /data/srv/current/config/crabserver

RUN echo "${USER} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/01-crab3

# copy running script files
COPY cicd/crabserver_pypi/run.sh cicd/crabserver_pypi/manage cicd/crabserver_pypi/entrypoint.sh /data/

# make sure all files is the same user/group as running user
RUN chown -R 1000:1000 /data

USER ${USER}
CMD /data/run.sh
