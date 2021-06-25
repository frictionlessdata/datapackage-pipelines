#!/usr/bin/env sh

sudo rm -rf tests/docker/data

! docker run -v `pwd`/tests/docker:/pipelines:rw frictionlessdata/datapackage-pipelines run ./test \
    && echo failed to run docker && exit 1

! ls -lah tests/docker/data/datapackage.json tests/docker/data/test.csv \
    && echo failed to find output files from docker run && exit 1

sudo rm -rf tests/docker/data

! docker run -d --name dpp -v `pwd`/tests/docker:/pipelines:rw frictionlessdata/datapackage-pipelines server-reload \
    && echo failed to start daemonized docker container && exit 1

for i in 1 2 3 4 5 6 7 8 9; do
    sleep 10
    ls -lah tests/docker/data/test.csv 2>/dev/null && break
    echo .
done

! ls -lah tests/docker/data/datapackage.json tests/docker/data/test.csv \
    && docker logs dpp && echo Failed to detect output data from daemonized docker container && exit 1

ls -lah tests/docker/data/test_package 2>/dev/null \
    && docker logs dpp && echo detected test_package data && exit 1

! docker exec dpp sh -c "cd lib; python3 setup.py install" \
    && echo failed to install docker test package && exit 1

! docker kill -s HUP dpp \
    && docker logs && echo failed to send HUP to docker && exit 1

for i in 1 2 3 4 5 6 7 8 9; do
    sleep 10
    ls -lah tests/docker/data/test_package/test.csv 2>/dev/null && break
    echo .
done

! ls -lah tests/docker/data/test_package/datapackage.json tests/docker/data/test_package/test.csv \
    && docker logs dpp && echo Failed to detect test package output data from daemonized docker container && exit 1

docker logs dpp

docker rm --force dpp

sudo rm -rf tests/docker

echo Great Success
exit 0
