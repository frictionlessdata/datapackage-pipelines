.PHONY: all install list lint release test version build


PACKAGE := $(shell grep '^PACKAGE =' setup.py | cut -d "'" -f2)
VERSION := $(shell head -n 1 $(PACKAGE)/VERSION)


all: list

install:
	pip install --upgrade -e .[develop]

install-speedup:
	pip install --upgrade -e .[develop,speedup]

list:
	@grep '^\.PHONY' Makefile | cut -d' ' -f2- | tr ' ' '\n'

lint:
	pylama $(PACKAGE)

release:
	bash -c '[[ -z `git status -s` ]]'
	git tag -a -m release $(VERSION)
	git push --tags

test:
	tox &&\
	tests/cli/test_cli_exit_codes.sh &&\
	tests/cli/test_cli_logs.sh &&\
	tests/cli/test_custom_formatters.sh &&\
	tests/cli/test_exclude_dirnames.sh &&\
	tests/cli/test_flow.sh

version:
	@echo $(VERSION)

build:
	docker pull frictionlessdata/datapackage-pipelines &&\
	docker build -t datapackage-pipelines --cache-from frictionlessdata/datapackage-pipelines .
