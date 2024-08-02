# build:
# cd CRABServer
# docker build -t filebeat-126-(date +%s) --file cicd/filebeat/Dockerfile .

FROM debian:12-slim

RUN apt update &&\
    apt install -y wget gnupg tini && \
    apt clean all


# install filebeat
RUN wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | apt-key add  - && \
  apt-get install apt-transport-https && \
  echo "deb https://artifacts.elastic.co/packages/oss-8.x/apt stable main" |  tee -a /etc/apt/sources.list.d/elastic-8.x.list && \
  apt-get update && apt-get install filebeat && \
  apt-get clean all

# user config
ENV USER=crab3
RUN useradd -m ${USER}
# for debuggin purpose
USER ${USER}

ENTRYPOINT ["tini", "--"]
CMD [ "/usr/share/filebeat/bin/filebeat", "--path.home", "/tmp/filebeat", \
      "-c", "/data/hostdisk/filebeat-crabtwfilebeat.yaml" ]

