#!/usr/bin/env sh

sudo rm -rf tests/docker/data

! docker run -v `pwd`/tests/docker:/pipelines:rw datapackage-pipelines run ./test \
    && echo failed to run docker && exit 1

! ls -lah tests/docker/data/datapackage.json tests/docker/data/test.csv \
    && echo failed to find output files from docker run && exit 1

sudo rm -rf tests/docker/data

! docker run -d --name dpp -v `pwd`/tests/docker:/pipelines:rw datapackage-pipelines server \
    && echo failed to start daemonized docker container && exit 1

for i in 1 2 3 4 5 6 7 8 9; do
    sleep 1
    ls -lah tests/docker/data 2>/dev/null && break
    echo .
done

docker logs dpp

! ls -lah tests/docker/data/datapackage.json tests/docker/data/test.csv \
    && echo Failed to detect outout data from daemonized docker container && exit 1

docker rm --force dpp

echo Great Success
exit 0
