#!/usr/bin/env bash

! dpp \
    && echo "test failed: dpp returned with non-zero exit code $?" && exit 1

dpp run ./tests/cli/raise-exception \
    && echo "test failed: exception in pipeline returned successful exit code" && exit 1

dpp run ./tests/cli/failure-no-errors \
    && echo "test failed: pipeline that failed without errors returned successful exit code" && exit 1

! dpp run ./tests/cli/success \
    && echo "test failed: success pipeline returned with non-zero exit code $?" && exit 1

dpp run --concurrency 4 \
        ./tests/cli/raise-exception,./tests/env/dummy/pipeline-test-data%,./tests/cli/failure-no-errors \
    && echo "test failed: concurrent run with failures returned successful exit code" && exit 1

! dpp run --concurrency 2 \
          ./tests/cli/success,./tests/cli/verbose-logs-with-sleep,./tests/env/dummy/pipeline-test-data% \
    && echo "test failed: concurrent run without failures returned non-zero exit code $?" && exit 1

echo "Great Success"
exit 0
