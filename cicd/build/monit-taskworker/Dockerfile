ARG TAG=latest
FROM registry.cern.ch/cmscrab/crabtaskworker:${TAG}

RUN source /data/srv/TaskManager/env.sh && \
  python3 -m ensurepip && \
  python3 -m pip install pyjwt


COPY ./src/script/Monitor/GenerateMONIT.py /data/srv/TaskManager/

COPY ./cicd/build/monit-taskworker/entrypoint.sh /entrypoint.sh

ENTRYPOINT [ "/bin/bash", "/entrypoint.sh" ]

CMD python3 /data/srv/TaskManager/GenerateMONIT.py
