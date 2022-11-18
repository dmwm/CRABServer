ARG BASEIMAGE=registry.cern.ch/cmsweb/crabserver:v3.221027
FROM $BASEIMAGE
USER 0
RUN mkdir /tmp/hotpatch
COPY manage.diff run.sh.diff Main.py.diff /tmp/hotpatch/
RUN cd /tmp/hotpatch \
    && patch /data/srv/current/config/crabserver/manage < manage.diff \
    && patch /data/run.sh < run.sh.diff \
    && patch $(find /data/srv/current/ -name Main.py) < Main.py.diff
USER 1000
