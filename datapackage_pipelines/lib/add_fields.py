import functools
import collections
import logging

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher

parameters, datapackage, resource_iterator = ingest()

resources = ResourceMatcher(parameters.get('resources'))
fields = parameters.get('fields', [])


def modify_datapackage(datapackage_):
    dp_resources = datapackage_.get('resources', [])
    for resource_ in dp_resources:
        if resources.match(resource_['name']):
            new_fields = [
                {'name': f['target'], 'type': f.get('type', 'any')} for f in fields
            ]
            resource_['schema']['fields'] += new_fields
    return datapackage_


def process_resource(rows):
    for row in rows:
        for field in fields:
            values = [
                row.get(c, field.get('nullas', 0)) for c in field.get('columns', [])
            ]
            formated_string = field.get('formated-string', '')
            constant = field.get('constant')
            new_col = AGGREGATORS[field['operation']].func(
                                    values, constant or formated_string, row)
            row[field['target']] = new_col
        yield row


def process_resources(resource_iterator_):
    for resource in resource_iterator_:
        spec = resource.spec
        if not resources.match(spec['name']):
            yield resource
        else:
            yield process_resource(resource)


Aggregator = collections.namedtuple('Aggregator', ['func'])

AGGREGATORS = {
    'sum': Aggregator(lambda values, fstr, row: sum(values)),
    'avg': Aggregator(lambda values, fstr, row: sum(values) / len(values)),
    'max': Aggregator(lambda values, fstr, row: max(values)),
    'min': Aggregator(lambda values, fstr, row: min(values)),
    'multiply': Aggregator(
        lambda values, fstr, row: functools.reduce(lambda x, y: x*y, values)),
    'constant': Aggregator(lambda values, fstr, row: fstr),
    'join': Aggregator(
        lambda values, fstr, row: ','.join([str(x) for x in values])),
    'format': Aggregator(lambda values, fstr, row: fstr.format(**row)),
}


spew(modify_datapackage(datapackage), process_resources(resource_iterator))
