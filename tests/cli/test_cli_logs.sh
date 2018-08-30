#!/usr/bin/env bash

TEMPDIR=`mktemp -d`

! script -ec "dpp run --verbose ./tests/cli/verbose-logs-with-sleep" $TEMPDIR/verbose_log && echo failed to run with --verbose && exit 1
cat -v $TEMPDIR/verbose_log | grep '\^\[\[[0-9][0-9]*A' && echo running with --verbose - found terminal escape sequences && exit 1

! script -ec "dpp run ./tests/cli/verbose-logs-with-sleep" $TEMPDIR/log && echo failed to run without verbose && exit 1
! cat -v $TEMPDIR/log | grep '\^\[\[[0-9][0-9]*A' && echo running without verbose - did not find terminal escape sequences && exit 1

! OUTPUT=`dpp run --verbose ./tests/cli/load-resource-progress-log 2>&1` && echo failed to run load-resource-progress && exit 1
for i in 2 4 6 8; do
    ! echo $OUTPUT | grep -q "loaded $i rows" && echo failed to detect load resource log && exit 1
done

rm -rf "${TEMPDIR}"

echo Great Success
exit 0
