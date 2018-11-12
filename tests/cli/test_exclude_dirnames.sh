#!/usr/bin/env bash

! dpp | grep ./tests/cli && echo missing tests/cli pipelines && exit 1
! dpp | grep ./samples/worldbank && echo missing samples pipelines && exit 1
! dpp | grep ./tests/env/ && echo missing tests/env pipelines && exit 1
! dpp | grep ./tests/docker/ && echo missing tests/docker pipelines && exit 1

echo "env
/samples
/tests/cli" > .dpp_spec_ignore

dpp | grep ./tests/cli && echo tests/cli pipelines not excluded && exit 1
dpp | grep ./samples/worldbank && echo samples pipelines not excluded && exit 1
dpp | grep ./tests/env/ && echo tests/env pipelines not excluded && exit 1
! dpp | grep ./tests/docker/ && echo missing tests/docker pipelines && exit 1

rm .dpp_spec_ignore

echo Great Success
exit 0
