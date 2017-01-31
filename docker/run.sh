#!/bin/sh
cd /pipelines 
if [ "$1" = "server" ]; then 
    export DPP_REDIS_HOST=127.0.0.1
    echo "Starting Server"
    redis-server --daemonize yes
    rm -f celeryd.pid
    python3 -m celery --detach -s /opt/schedule.db -b redis://localhost:6379/0 --concurrency=4 -B -A datapackage_pipelines.app -Q datapackage-pipelines -l INFO worker
    dpp serve
else
    /usr/local/bin/dpp "$@"
fi;


