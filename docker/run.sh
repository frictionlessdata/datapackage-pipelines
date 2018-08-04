#!/bin/sh

list_descendants() {
    local root_pid=$1
    local children=$(for PID in `ps -o pid,ppid | grep " $root_pid"'$'`; do [ "$PID" != "$root_pid" ] && echo $PID; done)
    for PID in $children; do list_descendants "$PID"; done
    [ "$children" != "" ] && echo "$children"
}

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
    dpp serve &
    DPP_SERVE_PID=$!
    sleep 5
    echo $DPP_SERVE_PID > /var/run/dpp-serve.pid
    wait $DPP_SERVE_PID
    rm -f /var/run/dpp-serve.pid
    exit 0
elif [ "$1" = "server-reload" ]; then
    trap 'echo reloading...; while ! /dpp/docker/run.sh stop-server; do echo .; sleep 1; done' HUP
    while true; do
        /dpp/docker/run.sh server &
        wait $!
    done
elif [ "$1" == "stop-server" ]; then
    DPP_SERVE_PID=`cat /var/run/dpp-serve.pid 2>/dev/null` && rm /var/run/dpp-serve.pid
    [ "$?" != "0" ] && echo missing dpp-serve.pid && exit 1
    DPP_SERVE_PIDS="$(list_descendants $DPP_SERVE_PID) $DPP_SERVE_PID"
    pstree -p
    echo collecting pids to terminate
    PIDS=""
    for PIDFILE in dpp-celeryd-worker dpp-celeryd-management dpp-celerybeat redis; do
        PID=`cat /var/run/$PIDFILE.pid 2>/dev/null` \
        && PIDS="$PIDS $(list_descendants $PID) $PID"
    done
    if [ "$PIDS" != "" ]; then
        echo sending TERM signal for pids: ${PIDS}
        for PID in $PIDS; do kill $PID; done
        echo sleeping ${DPP_RELOAD_GRACE_PERIOD:-5} seconds before sending KILL signal
        sleep ${DPP_RELOAD_GRACE_PERIOD:-5}
        for PID in $PIDS; do kill -9 $PID 2>/dev/null; done
        echo ensuring all PIDS were terminated
        for PID in $PIDS; do kill -0 $PID 2>/dev/null \
                          && kill -9 $PID 2>/dev/null \
                          && echo sleeping ${DPP_RELOAD_TERMINATE_PERIOD:-2} seconds to allow process $PID to be KILLed \
                          && sleep ${DPP_RELOAD_TERMINATE_PERIOD:-2} \
                          && kill -0 $PID 2>/dev/null && echo $PID not killed && exit 1; done
    fi
    for PIDFILE in dpp-celeryd-worker dpp-celeryd-management dpp-celerybeat redis; do
        rm -f /var/run/$PIDFILE.pid
    done
    echo sending TERM signal to dpp-serve and descendats
    kill $DPP_SERVE_PIDS 2>/dev/null
    kill -0 $DPP_SERVE_PID 2>/dev/null && echo waiting up to 5 seconds to let dpp-serve to be killed peacefully \
    && for i in 0 1 2 3 4 5; do ! kill -0 $DPP_SERVE_PID 2>/dev/null || sleep 1; done
    kill -9 $DPP_SERVE_PIDS
    sleep ${DPP_RELOAD_TERMINATE_PERIOD:-2} && kill -0 $DPP_SERVE_PID 2>/dev/null && echo dpp serve not killed && exit 1
    echo killed server PID $DPP_SERVE_PID
    pstree -p
    exit 0
else
    /usr/local/bin/dpp "$@"
fi;
