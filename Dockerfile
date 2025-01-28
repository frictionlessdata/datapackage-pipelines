FROM python:3.9-alpine

RUN apk --update --no-cache --virtual=build-dependencies add \
        build-base python3-dev \libxml2-dev libxslt-dev postgresql-dev leveldb leveldb-dev  && \
    apk --update --no-cache add libstdc++ redis libpq && \
    mkdir -p /run/redis && mkdir -p /var/run/dpp && \
    pip install psycopg2 datapackage-pipelines-github datapackage-pipelines-aws datapackage-pipelines-sourcespec-registry 

ADD . /dpp/

RUN pip install -U /dpp/[speedup] && \
    mkdir -p /var/redis && chmod 775 /var/redis && chown redis:redis /var/redis

ENV DPP_NUM_WORKERS=4
ENV DPP_REDIS_HOST=127.0.0.1
ENV DPP_CELERY_BROKER=redis://localhost:6379/6

EXPOSE 5000
WORKDIR /pipelines/
ENTRYPOINT ["/dpp/docker/run.sh"]


