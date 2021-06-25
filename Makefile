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
	docker login -u "${DOCKER_USERNAME}" -p "${DOCKER_PASSWORD}"
	docker pull frictionlessdata/datapackage-pipelines:latest &&\
	docker build -t frictionlessdata/datapackage-pipelines:latest --cache-from frictionlessdata/datapackage-pipelines . &&\
	docker build -t frictionlessdata/datapackage-pipelines:latest-alpine --cache-from frictionlessdata/datapackage-pipelines . &&\
	docker build -t frictionlessdata/datapackage-pipelines:${VERSION} --cache-from frictionlessdata/datapackage-pipelines . &&\
	docker build -t frictionlessdata/datapackage-pipelines:${VERSION}-alpine --cache-from frictionlessdata/datapackage-pipelines . &&\
	docker pull frictionlessdata/datapackage-pipelines:latest-slim &&\
	docker build -t frictionlessdata/datapackage-pipelines:latest-slim -f Dockerfile.slim --cache-from frictionlessdata/datapackage-pipelines:latest-slim . &&\
	docker build -t frictionlessdata/datapackage-pipelines:${VERSION}-slim -f Dockerfile.slim --cache-from frictionlessdata/datapackage-pipelines:latest-slim .


deploy-latest:
	docker login -u "${DOCKER_USERNAME}" -p "${DOCKER_PASSWORD}" &&\
	docker push frictionlessdata/datapackage-pipelines:latest &&\
	docker push frictionlessdata/datapackage-pipelines:latest-alpine &&\
	docker push frictionlessdata/datapackage-pipelines:latest-slim

deploy-tags:
	docker login -u "${DOCKER_USERNAME}" -p "${DOCKER_PASSWORD}" &&\
	docker push frictionlessdata/datapackage-pipelines:${VERSION} &&\
	docker push frictionlessdata/datapackage-pipelines:${VERSION}-alpine &&\
	docker push frictionlessdata/datapackage-pipelines:${VERSION}-slim

deploy-pip:
	rm -rf dist/ || true
	pip install wheel twine
	python setup.py sdist bdist_wheel
	python -m twine upload dist/*