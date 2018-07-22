#!/usr/bin/env bash

pip install -e tests/cli
pip install openpyxl
rm ./tests/cli/custom_formatters/datapackage.json \
   ./tests/cli/custom_formatters/my-spiffy-resource.xlsx \
   ./tests/cli/custom_formatters/sample.csv 2>/dev/null

! dpp run ./tests/cli/custom-formatters \
    && echo failed to run custom formatters pipeline && exit 1

! ls -lah tests/cli/custom_formatters/my-spiffy-resource.xlsx \
          tests/cli/custom_formatters/sample.csv \
          tests/cli/custom_formatters/datapackage.json \
    && echo missing custom formatters output files && exit 1

echo Great Success
exit 0
