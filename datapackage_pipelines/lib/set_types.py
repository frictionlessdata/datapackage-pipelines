import re
import logging

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher
import jsontableschema

parameters, datapackage, resource_iterator = ingest()

resources = ResourceMatcher(parameters.get('resources'))
types = parameters.get('types', {})


def match_fields(field_name_re, expected):
    def filt(field):
        return (field_name_re.fullmatch(field['name']) is not None) is expected
    return filt


def process_datapackage(datapackage_):
    for resource in datapackage_['resources']:
        name = resource['name']
        if not resources.match(name):
            continue

        fields = resource.setdefault('schema', {}).get('fields', [])
        for field_name, field_definition in types.items():
            field_name_re = re.compile(field_name)
            if field_definition is not None:
                filtered_fields = list(
                    filter(match_fields(field_name_re, True), fields)
                )
                for field in filtered_fields:
                    field.update(field_definition)
                assert len(filtered_fields) > 0, \
                    "No field found matching %r" % field_name
            else:
                fields = list(
                    filter(match_fields(field_name_re, False), fields)
                )

        resource['schema']['fields'] = fields


def process_resource(spec, rows):
    schema = spec['schema']
    jts = jsontableschema.Schema(schema)
    field_names = list(map(lambda f: f['name'], schema['fields']))
    for row in rows:
        flattened_row = [row.get(name) for name in field_names]
        try:
            flattened_row = jts.cast_row(flattened_row)
        except Exception:
            logging.error('Failed to cast row %r', flattened_row)
            raise
        row = dict(zip(field_names, flattened_row))
        yield row


def process_resources(resource_iterator_):
    for resource in resource_iterator_:
        spec = resource.spec
        if not resources.match(spec['name']):
            yield resource
        else:
            yield process_resource(spec, resource)


process_datapackage(datapackage)

spew(datapackage, process_resources(resource_iterator))
