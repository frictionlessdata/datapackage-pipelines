#!/usr/bin/env bash

cd tests/cli

rm -rf test_flow_data

TEMPFILE=`mktemp`

set -o pipefail
! dpp run --verbose ./dataflows >/dev/stdout 2>&1 | tee $TEMPFILE && echo failed to run dataflows pipeline && exit 1
set +o pipefail
! cat "${TEMPFILE}" | grep "hello dataflows" && echo dataflows output is missing && exit 1
rm $TEMPFILE

ACTUAL=$(cat -A test_flow_data/res_1.csv)
EXPECTED="foo^M$
bar^M$
baz^M$"

! [ "$ACTUAL" == "$EXPECTED" ] && echo unexpected output data from dataflows pipeline && exit 1

echo Great Success
exit 0
