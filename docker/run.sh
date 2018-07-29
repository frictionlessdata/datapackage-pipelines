#!/bin/sh

if [ "$1" = "server" ]; then 
    export DPP_REDIS_HOST=127.0.0.1
    export DPP_CELERY_BROKER=redis://localhost:6379/6
    echo "Starting Server"
    redis-server /etc/redis.conf --daemonize yes --dir /var/redis
    until [ `redis-cli ping | grep -c PONG` = 1 ]; do echo "Waiting 1s for Redis to load"; sleep 1; done
    rm -f /var/run/dpp-celerybeat.pid /var/run/dpp-celeryd-management.pid /var/run/dpp-celeryd-worker.pid
    python /dpp/docker/github_config.py
    dpp init

    echo "Deleting `redis-cli -n 6 KEYS '*' | wc -l` keys"
    redis-cli -n 6 FLUSHDB
    echo "Remaining `redis-cli -n 6 KEYS '*' | wc -l` keys"

    SCHEDULER=1 python3 -m celery -b $DPP_CELERY_BROKER -A datapackage_pipelines.app -l INFO --pidfile=/var/run/dpp-celerybeat.pid beat &
    python3 -m celery -b $DPP_CELERY_BROKER --concurrency=1 -A datapackage_pipelines.app -Q datapackage-pipelines-management -l INFO --pidfile=/var/run/dpp-celeryd-management.pid worker &
    python3 -m celery -b $DPP_CELERY_BROKER --concurrency=4 -A datapackage_pipelines.app -Q datapackage-pipelines -l INFO --pidfile=/var/run/dpp-celeryd-worker.pid worker &
    exec dpp serve
elif [ "$1" = "server-reload" ]; then
    trap 'kill -9 `cat /var/run/dpp-server.pid`' HUP
    while true; do
        PIDS=""
        for PIDFILE in dpp-celerybeat dpp-celeryd-management dpp-celeryd-worker redis; do
            PID=`cat /var/run/$PIDFILE.pid 2>/dev/null` && rm /var/run/$PIDFILE.pid \
            && kill -0 $PID 2>/dev/null && PIDS="$PIDS $PID" && kill $PID 2>/dev/null
        done
        if [ "$PIDS" != "" ]; then
            sleep ${DPP_RELOAD_GRACE_PERIOD:-2}
            for PID in $PIDS; do kill -9 $PID 2>/dev/null; done
            for PID in $PIDS; do kill -0 $PID 2>/dev/null && sleep ${DPP_RELOAD_TERMINATE_PERIOD:-2} \
                              && kill -0 $PID 2>/dev/null && echo $PID not killed && exit 1; done
        fi
        /dpp/docker/run.sh server &
        DPP_SERVER_PID=$!
        echo $DPP_SERVER_PID > /var/run/dpp-server.pid
        wait $DPP_SERVER_PID
        rm /var/run/dpp-server.pid
    done
else
    /usr/local/bin/dpp "$@"
fi;
