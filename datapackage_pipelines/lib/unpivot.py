import copy
import re

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher

parameters, datapackage, resource_iterator = ingest()

resources = ResourceMatcher(parameters.get('resources'))
unpivot_fields = parameters.get('unpivot')
extra_keys = parameters.get('extraKeyFields')
extra_value = parameters.get('extraValueField')


def match_fields(field_name_re, expected):
    def filt(field):
        return (field_name_re.fullmatch(field['name']) is not None) is expected
    return filt


def process_datapackage(datapackage_):
    unpivot_fields_without_regex = []
    for resource in datapackage_['resources']:
        name = resource['name']
        if not resources.match(name):
            continue

        if 'schema' not in resource:
            continue

        fields = resource['schema'].get('fields', [])

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
                original_key_values = u_field['keys']  # With regex
                new_key_values = {}
                for key in original_key_values:
                    new_val = original_key_values[key]
                    if isinstance(new_val, str):
                        new_val = re.sub(
                            u_field['name'], new_val, field_to_pivot['name'])
                    new_key_values[key] = new_val
                    field_to_pivot['keys'] = new_key_values
                unpivot_fields_without_regex.append(field_to_pivot)

        fields_to_keep = [f['name'] for f in fields]
        fields.extend(extra_keys)
        fields.append(extra_value)
        resource['schema']['fields'] = fields
    return unpivot_fields_without_regex, fields_to_keep


def unpivot(rows, fields_to_unpivot_, fields_to_keep_):
    for row in rows:
        for unpivot_field in fields_to_unpivot_:
            new_row = copy.deepcopy(unpivot_field['keys'])
            for field in fields_to_keep_:
                new_row[field] = row[field]
            new_row[extra_value['name']] = row.get(unpivot_field['name'])
            yield new_row


def process_resources(resource_iterator_, fields_to_unpivot, fields_to_keep):
    for resource in resource_iterator_:
        spec = resource.spec
        if not resources.match(spec['name']):
            yield resource
        else:
            yield unpivot(resource, fields_to_unpivot, fields_to_keep)


old_fields, keep_fields = process_datapackage(datapackage)

spew(datapackage, process_resources(resource_iterator, old_fields, keep_fields))
