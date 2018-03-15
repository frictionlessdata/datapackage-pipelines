#!/usr/bin/env bash

TEMPDIR=`mktemp -d`

! script -ec "dpp run --verbose ./tests/cli/verbose-logs-with-sleep" $TEMPDIR/log && echo failed to run && exit 1

# dump the non-escaped sequences to visualize the problem
# TODO: figure out how to test for missing log line due to terminal escape sequences
cat -v $TEMPDIR/log

rm -rf "${TEMPDIR}"

echo Great Success?
exit 0
