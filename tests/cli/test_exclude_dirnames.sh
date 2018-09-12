#!/usr/bin/env bash

TEMPDIR=`mktemp -d`

pushd $TEMPDIR
    mkdir ignored
    echo 'ignored_pipeline: {pipeline: []}' > ignored/pipeline-spec.yaml

    ! dpp | grep ignored/ignored_pipeline &&\
        echo 'no DPP_EXCLUDE_DIRNAMES but ignored pipeline missing' &&\
        exit 1

    DPP_EXCLUDE_DIRNAMES=igno'*' dpp | grep ignored/ignored_pipeline &&\
        echo 'ignored pipeline exists' &&\
        exit 1
popd

rm -rf $TEMPDIR

echo Great Success
exit 0
