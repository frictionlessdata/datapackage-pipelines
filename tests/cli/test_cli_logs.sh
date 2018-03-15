#!/usr/bin/env bash

TEMPDIR=`mktemp -d`

! script -ec "dpp run --verbose ./tests/cli/verbose-logs-with-sleep" $TEMPDIR/verbose_log && echo failed to run with --verbose && exit 1
cat -v $TEMPDIR/verbose_log | grep '\^\[\[[0-9][0-9]*A' && echo running with --verbose - found terminal escape sequences && exit 1

! script -ec "dpp run ./tests/cli/verbose-logs-with-sleep" $TEMPDIR/log && echo failed to run without verbose && exit 1
! cat -v $TEMPDIR/log | grep '\^\[\[[0-9][0-9]*A' && echo running without verbose - did not find terminal escape sequences && exit 1

rm -rf "${TEMPDIR}"

echo Great Success
exit 0
