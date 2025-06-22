.PHONY: all install list lint release test version build


PACKAGE := $(shell grep '^PACKAGE =' setup.py | cut -d "'" -f2)
VERSION := $(shell head -n 1 $(PACKAGE)/VERSION)
DOCKER_IMAGE := ghcr.io/whiletrue-industries/datapackage-pipelines


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
	docker pull $(DOCKER_IMAGE):latest &&\
	docker build -t $(DOCKER_IMAGE):latest --cache-from $(DOCKER_IMAGE) . &&\
	docker build -t $(DOCKER_IMAGE):latest-alpine --cache-from $(DOCKER_IMAGE) . &&\
	docker build -t $(DOCKER_IMAGE):${VERSION} --cache-from $(DOCKER_IMAGE) . &&\
	docker build -t $(DOCKER_IMAGE):${VERSION}-alpine --cache-from $(DOCKER_IMAGE) . &&\
	docker pull $(DOCKER_IMAGE):latest-slim &&\
	docker build -t $(DOCKER_IMAGE):latest-slim -f Dockerfile.slim --cache-from $(DOCKER_IMAGE):latest-slim . &&\
	docker build -t $(DOCKER_IMAGE):${VERSION}-slim -f Dockerfile.slim --cache-from $(DOCKER_IMAGE):latest-slim .


deploy-latest:
	docker login -u "${DOCKER_USERNAME}" -p "${DOCKER_PASSWORD}" &&\
	docker push $(DOCKER_IMAGE):latest &&\
	docker push $(DOCKER_IMAGE):latest-alpine &&\
	docker push $(DOCKER_IMAGE):latest-slim

deploy-tags:
	docker login -u "${DOCKER_USERNAME}" -p "${DOCKER_PASSWORD}" &&\
	docker push $(DOCKER_IMAGE):${VERSION} &&\
	docker push $(DOCKER_IMAGE):${VERSION}-alpine &&\
	docker push $(DOCKER_IMAGE):${VERSION}-slim

deploy-pip:
	rm -rf dist/ || true
	pip install wheel twine
	python setup.py sdist bdist_wheel
	python -m twine upload dist/*