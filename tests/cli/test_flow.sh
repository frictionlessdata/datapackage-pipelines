#!/usr/bin/env bash

cd tests/cli

rm -rf test_flow_data

TEMPFILE=`mktemp`

set -o pipefail
! dpp run --verbose ./dataflows >/dev/stdout 2>&1 | tee $TEMPFILE && echo failed to run dataflows pipeline && exit 1
set +o pipefail
! cat "${TEMPFILE}" | grep "hello dataflows" && echo dataflows output is missing && exit 1
! cat "${TEMPFILE}" | grep "'foo_values': 9" && echo dataflows output is missing stats && exit 1
rm $TEMPFILE

! diff test_flow_data/sample.csv expected_flow_data.csv && echo unexpected output data && exit 1

echo Great Success
exit 0
