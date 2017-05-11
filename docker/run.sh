#!/bin/sh
if [ "$1" = "server" ]; then 
    export DPP_REDIS_HOST=127.0.0.1
    echo "Starting Server"
    redis-server /etc/redis.conf --daemonize yes --dir /var/redis
    until [ `redis-cli ping | grep -c PONG` = 1 ]; do echo "Waiting 1s for Redis to load"; sleep 1; done
    rm -f celeryd.pid
    rm -f celerybeat.pid
    python /dpp/docker/github_config.py
    dpp init
    python3 -m celery -b redis://localhost:6379/6 -A datapackage_pipelines.app -l INFO beat &
    python3 -m celery -b redis://localhost:6379/6 --concurrency=1 -A datapackage_pipelines.app -Q datapackage-pipelines-management -l INFO worker &
    python3 -m celery -b redis://localhost:6379/6 --concurrency=4 -A datapackage_pipelines.app -Q datapackage-pipelines -l INFO worker &
    dpp serve
else
    /usr/local/bin/dpp "$@"
fi;


