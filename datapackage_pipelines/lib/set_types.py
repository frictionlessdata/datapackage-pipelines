import re

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher

parameters, datapackage, resource_iterator = ingest()

resources = ResourceMatcher(parameters.get('resources'))
types = parameters.get('types')

for resource in datapackage['resources']:

    name = resource['name']
    if not resources.match(name):
        continue

    fields = resource.setdefault('schema', {}).get('fields', [])
    for field_name, field_definition in types.items():
        field_name_re = re.compile(field_name)
        if field_definition is not None:
            filtered_fields = list(
                filter(
                    lambda f: field_name_re.fullmatch(f['name']) is not None,  # pylint: disable=cell-var-from-loop
                    fields
                )
            )
            for field in filtered_fields:
                field.update(field_definition)
            assert len(filtered_fields) > 0, \
                "No field found matching %r" % field_name
        else:
            fields = list(
                filter(
                    lambda f: field_name_re.fullmatch(f['name']) is None,  # pylint: disable=cell-var-from-loop
                    fields
                )
            )

    resource['schema']['fields'] = fields

spew(datapackage, resource_iterator)
