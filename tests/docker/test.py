from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING
import datetime

parameters, datapackage, resources, stats = ingest() + ({},)


datapackage['resources'] = [{'name': 'test', 'path': 'test.csv',
                             PROP_STREAMING: True,
                             'schema': {'fields': [{'name': 'a', 'type': 'string'}]}}]


spew(datapackage, [({'a': 'foo'}, {'a': 'bar'})], {'last_run_time': str(datetime.datetime.now())})
