#!/usr/bin/env bash

pip install -e tests/cli
pip install openpyxl

OUTPUT_FILES="tests/cli/custom_formatters/my-spiffy-resource.xlsx
              tests/cli/custom_formatters/sample.csv
              tests/cli/custom_formatters/datapackage.json
              tests/cli/custom_formatters/datapackage.zip"

rm -f $OUTPUT_FILES

! dpp run ./tests/cli/custom-formatters && echo failed to run custom formatters pipeline && exit 1

! ls -lah $OUTPUT_FILES && echo missing custom formatters output files && exit 1

validate_lannisters() {
    NUM_LANNISTERS=$(python - <<EOF
import openpyxl
wb=openpyxl.load_workbook('my-spiffy-resource.xlsx')
print(len([True for row in wb.active.rows for cell in row if cell.value == 'Lannister']))
EOF
)
    [ "${NUM_LANNISTERS}" != "6" ] && echo invalid number of Lannisters && return 1
    return 0
}

pushd tests/cli/custom_formatters >/dev/null
    ! validate_lannisters && exit 1
popd >/dev/null

DATAPACKAGE_ZIP=`pwd`/tests/cli/custom_formatters/datapackage.zip
TEMP_DIR=`mktemp -d`
pushd $TEMP_DIR >/dev/null
unzip "${DATAPACKAGE_ZIP}"
    ! validate_lannisters && exit 1
popd >/dev/null

rm -rf $TEMP_DIR
rm -f $OUTPUT_FILES

echo Great Success
exit 0
