#!/usr/bin/env bash

cd tests/cli

TEMPFILE=`mktemp`

set -o pipefail
! dpp run --verbose ./exec >/dev/stdout 2>&1 | tee $TEMPFILE && echo failed to run exec pipeline && exit 1
set +o pipefail

! cat "${TEMPFILE}" | grep "test_exec.sh" && echo exec output is missing && exit 1
! cat "${TEMPFILE}" | grep "__EXEC_PROCESSOR_PATH" && echo exec output is missing && exit 1
rm $TEMPFILE

TEMPFILE=`mktemp`

rm -f test_exec_shell_tempfile

set -o pipefail
! dpp run --verbose ./exec_shell >/dev/stdout 2>&1 | tee $TEMPFILE && echo failed to run exec_shell pipeline && exit 1
set +o pipefail

! [ "$(cat $(cat test_exec_shell_tempfile))" == "test" ] \
    && echo unexecpted data && exit 1

rm $(cat test_exec_shell_tempfile)
rm test_exec_shell_tempfile

echo Great Success
exit 0
