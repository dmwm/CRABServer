# `buildtools` container

This Dockerfile is for build `registry.cern.ch/cmscrab/buildtools` container.

## Build

Simply run `docker build` to build this container.

```bash
docker build -t registry.cern.ch/cmscrab/buildtools:latast -f Dockerfile .
```

Then test it by running usual `docker run` command.

If everything is alright, you can use [buildAndPush.sh](./buildAndPush.sh) build, tags and push to CERN's registry. This script will tag the container with `latest` and `${today}` (for example, `240101`) tag.
