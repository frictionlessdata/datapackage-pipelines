FROM python:3.5-alpine

ADD . /dpp/

RUN apk --update --virtual=build-dependencies add build-base python3-dev libxml2-dev libxslt-dev && \
    apk --update add libstdc++ redis && \
    pip install /dpp/ && \
    apk del build-dependencies && \
    rm -rf /var/cache/apk/*

EXPOSE 5000

ENTRYPOINT ["/dpp/docker/run.sh"]


