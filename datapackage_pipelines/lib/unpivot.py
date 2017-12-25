import re

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher

parameters, datapackage, resource_iterator = ingest()

resources = ResourceMatcher(parameters.get('resources'))
unpivot_fields = parameters.get('unpivot')
keys_ = parameters.get('extraKeyFields')
values_ = parameters.get('extraValueField')


def match_fields(field_name_re, expected):
    def filt(field):
        return (field_name_re.fullmatch(field['name']) is not None) is expected
    return filt


def process_datapackage(datapackage_):
    for resource in datapackage_['resources']:
        name = resource['name']
        if not resources.match(name):
            continue

        if 'schema' not in resource:
            continue

        fields = resource.setdefault('schema', {}).get('fields', [])
        for field in unpivot_fields:
            field_name_re = re.compile(field['name'])
            fields = list(
                filter(match_fields(field_name_re, False), fields)
            )
        fields.extend(keys_)
        fields.append(values_)
        resource['schema']['fields'] = fields


def parse_field(schema, field_name, field_val):
        if field_val is None:
            return None
        for field_meta in schema['fields']:
            if field_meta['name'] == field_name:
                if field_meta['type'] == 'number':
                    return float(field_val)
                elif field_meta['type'] == 'integer':
                    return int(field_val)
        return field_val


def unpivot(spec, rows):
    schema = spec['schema']
    field_names = list(map(lambda f: f['name'], schema['fields']))
    for row in rows:
        for row_name in row.keys():
            for unpivot_field in unpivot_fields:
                field_name_re = re.compile(unpivot_field['name'])
                if field_name_re.fullmatch(row_name) is not None:
                    new_row = {}
                    for field_name in field_names:
                        from_row = row.get(field_name)
                        key_ = unpivot_field['keys'].get(field_name)
                        from_spec = re.sub(unpivot_field['name'], str(key_), row_name)
                        if from_spec == 'None':
                            from_spec = key_
                        from_spec = parse_field(schema, field_name, from_spec)
                        val = row.get(row_name)
                        new_row[field_name] = from_row or from_spec or val
                    yield new_row


def process_resources(resource_iterator_):
    for resource in resource_iterator_:
        spec = resource.spec
        if not resources.match(spec['name']):
            yield resource
        else:
            yield unpivot(spec, resource)


process_datapackage(datapackage)

spew(datapackage, process_resources(resource_iterator))
