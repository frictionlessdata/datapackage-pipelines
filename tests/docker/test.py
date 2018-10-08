from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING
import datetime

parameters, datapackage, resources, stats = tuple(ingest()) + ({},)


if parameters.get('test-package'):
    from dpp_docker_test import DPP_DOCKER_TEST
    assert DPP_DOCKER_TEST


datapackage['resources'] = [{'name': 'test', 'path': 'test.csv',
                             PROP_STREAMING: True,
                             'schema': {'fields': [{'name': 'a', 'type': 'string'}]}}]


spew(datapackage, [({'a': 'foo'}, {'a': 'bar'})], {'last_run_time': str(datetime.datetime.now())})
