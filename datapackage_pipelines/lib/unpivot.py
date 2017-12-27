import copy
import re

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher

parameters, datapackage, resource_iterator = ingest()

resources = ResourceMatcher(parameters.get('resources'))
unpivot_fields = parameters.get('unpivot')
keys_ = parameters.get('extraKeyFields')
values_ = parameters.get('extraValueField')
unpivot_fields_without_regex = []


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

        for u_field in unpivot_fields:
            field_name_re = re.compile(u_field['name'])
            fields_to_pivot = (list(
                filter(match_fields(field_name_re, True), fields)
            ))
            fields = list(
                filter(match_fields(field_name_re, False), fields)
            )

            # handle with regex
            for field_to_pivot in fields_to_pivot:
                oiginal_key_values = u_field['keys']  # With regex
                new_key_values = {}
                for key in oiginal_key_values:
                    new_val = re.sub(
                        u_field['name'],
                        str(oiginal_key_values[key]),
                        field_to_pivot['name'])
                    # parse value to correct type
                    new_key_values[key] = parse_field(
                        keys_, key, new_val)
                    field_to_pivot['keys'] = new_key_values
                unpivot_fields_without_regex.append(field_to_pivot)

        fields.extend(keys_)
        fields.append(values_)
        resource['schema']['fields'] = fields


def parse_field(schema, field_name, field_val):
        if field_val is None:
            return None
        for field_meta in schema:
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
        for unpivot_field in unpivot_fields_without_regex:
            new_row = copy.deepcopy(unpivot_field['keys'])
            for field in field_names:
                if field in new_row:
                    continue
                from_row = row.get(field)
                val = row.get(unpivot_field['name'])
                new_row[field] = from_row or val
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
