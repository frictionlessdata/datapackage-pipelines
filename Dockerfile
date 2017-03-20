FROM python:3.5-alpine

ADD . /dpp/

RUN apk --update --no-cache --virtual=build-dependencies add build-base python3-dev libxml2-dev libxslt-dev postgresql-dev && \
    apk --update --no-cache add libstdc++ redis libpq && \
    pip install psycopg2 && \
    pip install /dpp/ && \
    apk del build-dependencies && \
    rm -rf /var/cache/apk/*  && \
    mkdir -p /var/redis && chmod 775 /var/redis && chown redis.redis /var/redis

EXPOSE 5000
WORKDIR /pipelines/
ENTRYPOINT ["/dpp/docker/run.sh"]


