#FROM python:3.6-alpine
FROM rcarmo/alpine-python:3.6.1

ADD . /dpp/

RUN apk --update --no-cache --virtual=build-dependencies add \
        build-base python3-dev \libxml2-dev libxslt-dev postgresql-dev  && \
    apk --update --no-cache add libstdc++ redis libpq && \
    apk --repository http://dl-3.alpinelinux.org/alpine/edge/testing/ --update add leveldb leveldb-dev && \
    pip install psycopg2 datapackage-pipelines-github && \
    pip install -U /dpp/[speedup] && \
    apk del build-dependencies && \
    rm -rf /var/cache/apk/*  && \
    mkdir -p /var/redis && chmod 775 /var/redis && chown redis.redis /var/redis

EXPOSE 5000
WORKDIR /pipelines/
ENTRYPOINT ["/dpp/docker/run.sh"]


