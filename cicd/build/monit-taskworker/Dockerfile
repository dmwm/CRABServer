ARG TAG=latest
FROM registry.cern.ch/cmscrab/crabtaskworker:${TAG}

RUN source /data/srv/TaskManager/env.sh && \
  python3 -m ensurepip && \
  python3 -m pip install pyjwt

COPY GenerateMONIT.py /data/srv/TaskManager/

COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT [ "/bin/bash", "/entrypoint.sh" ]

CMD python3 /data/srv/TaskManager/GenerateMONIT.py
