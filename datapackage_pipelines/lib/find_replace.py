import re

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher

parameters, datapackage, resource_iterator = ingest()

resources = ResourceMatcher(parameters.get('resources'))
fields = parameters.get('fields', [])


def process_resource(rows):
    for row in rows:
        for field in fields:
            for pattern in field.get('patterns', []):
                row[field['name']] = re.sub(
                    str(pattern['find']),
                    str(pattern['replace']),
                    str(row[field['name']]))
        yield row


def process_resources(resource_iterator_):
    for resource in resource_iterator_:
        spec = resource.spec
        if not resources.match(spec['name']):
            yield resource
        else:
            yield process_resource(resource)


spew(datapackage, process_resources(resource_iterator))
