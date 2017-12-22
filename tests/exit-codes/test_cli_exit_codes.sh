#!/usr/bin/env bash

! dpp \
    && echo "test failed: dpp returned with non-zero exit code $?" && exit 1

dpp run ./tests/exit-codes/raise-exception \
    && echo "test failed: exception in pipeline returned successful exit code" && exit 1

dpp run ./tests/exit-codes/failure-no-errors \
    && echo "test failed: pipeline that failed without errors returned successful exit code" && exit 1

! dpp run ./tests/exit-codes/success \
    && echo "test failed: success pipeline returned with non-zero exit code $?" && exit 1

echo "Great Success"
exit 0
