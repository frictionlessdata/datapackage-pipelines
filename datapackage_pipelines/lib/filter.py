from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher

import operator

parameters, datapackage, resource_iterator = ingest()

resources = ResourceMatcher(parameters.get('resources'))
equals = parameters.get('in', [])
not_equals = parameters.get('out', [])

conditions = [
    (operator.eq, k, v)
    for o in equals
    for k, v in o.items()
] + [
    (operator.ne, k, v)
    for o in not_equals
    for k, v in o.items()
]


def process_resource(rows):
    for row in rows:
        if any(func(row[k], v) for func, k, v in conditions):
            yield row


def process_resources(resource_iterator_):
    for resource in resource_iterator_:
        spec = resource.spec
        if not resources.match(spec['name']):
            yield resource
        else:
            yield process_resource(resource)


spew(datapackage, process_resources(resource_iterator))
