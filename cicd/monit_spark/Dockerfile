FROM registry.cern.ch/cmsmonitoring/cmsmon-spark:v0.5.0.1 

## build with from dmwm/CRABServer, root directory
# docker buildx build -t registry.cern.ch/cmscrab/crabspark:(date +%s) -f cicd/monit_spark/Dockerfile .

RUN yum install -y \
    tini \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN mkdir -p /data/srv/spark/
COPY ./src/script/Monitor/crab-spark/workdir/osearch.py \
     ./src/script/Monitor/crab-spark/workdir/bootstrap.sh \
     ./src/script/Monitor/crab-spark/cronjobs/crab_condor_daily.py \
     ./src/script/Monitor/crab-spark/cronjobs/crab_tape_recall_rules_history_daily.py \
     ./src/script/Monitor/crab-spark/cronjobs/run_spark.sh \
     ./src/script/Monitor/crab-spark/cronjobs/crab_data_daily.py \
     ./src/script/Monitor/crab-spark/cronjobs/crab_tape_recall_updated_rules_daily.py \
     /data/srv/spark

ENTRYPOINT ["tini", "--"]

CMD ["echo", "no default script for spark docker image"]


