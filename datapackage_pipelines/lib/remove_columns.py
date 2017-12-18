import copy
import logging

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher

parameters, datapackage, resource_iterator = ingest()

resources = ResourceMatcher(parameters.get('resources'))
columns = parameters.get('columns', [])


def delete_fields(datapackage_):
    dp_resources = datapackage_.get('resources', [])
    for resource_ in dp_resources:
        if resources.match(resource_['name']):
            fields = resource_['schema'].get('fields', [])
            assert all([col in [f['name'] for f in fields] for col in columns]), \
                "Can't remove non-existing column(s)"
            new_fields = [f for f in fields if f['name'] not in columns]
            resource_['schema']['fields'] = new_fields
    return datapackage_


def process_resource(rows):
    for row in rows:
        for column in columns:
            del row[column]
        yield row


def process_resources(resource_iterator_):
    for resource in resource_iterator_:
        spec = resource.spec
        if not resources.match(spec['name']):
            yield resource
        else:
            yield process_resource(resource)


spew(delete_fields(datapackage), process_resources(resource_iterator))
