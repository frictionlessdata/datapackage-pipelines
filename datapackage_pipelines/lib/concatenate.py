import itertools

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher
from datapackage_pipelines.utilities.resources import PROP_STREAMING

parameters, datapackage, resource_iterator = ingest()

sources = ResourceMatcher(parameters.get('sources'))

target = parameters.get('target', {})
if 'name' not in target:
    target['name'] = 'concat'
if 'path' not in target:
    target['path'] = 'data/' + target['name'] + '.csv'
target.update(dict(
    mediatype='text/csv',
    schema=dict(fields=[], primaryKey=[]),
))
target[PROP_STREAMING] = True

fields = parameters['fields']

# Create mapping between source field names to target field names
field_mapping = {}
for target_field, source_fields in fields.items():
    if source_fields is not None:
        for source_field in source_fields:
            if source_field in field_mapping:
                raise RuntimeError('Duplicate appearance of %s (%r)' % (source_field, field_mapping))
            field_mapping[source_field] = target_field

    if target_field in field_mapping:
        raise RuntimeError('Duplicate appearance of %s' % target_field)

    field_mapping[target_field] = target_field

# Create the schema for the target resource
needed_fields = sorted(fields.keys())
for resource in datapackage['resources']:
    if not sources.match(resource['name']):
        continue

    schema = resource.get('schema', {})
    pk = schema.get('primaryKey', [])
    for field in schema.get('fields', []):
        orig_name = field['name']
        if orig_name in field_mapping:
            name = field_mapping[orig_name]
            if name not in needed_fields:
                continue
            if orig_name in pk:
                target['schema']['primaryKey'].append(name)
            target['schema']['fields'].append(field)
            field['name'] = name
            needed_fields.remove(name)

if len(target['schema']['primaryKey']) == 0:
    del target['schema']['primaryKey']

for name in needed_fields:
    target['schema']['fields'].append(dict(
        name=name, type='string'
    ))

# Update resources in datapackage (make sure they are consecutive)
prefix = True
suffix = False
num_concatenated = 0
new_resources = []
for resource in datapackage['resources']:
    name = resource['name']
    match = sources.match(name)
    if prefix:
        if match:
            prefix = False
            num_concatenated += 1
        else:
            new_resources.append(resource)
    elif suffix:
        assert not match
        new_resources.append(resource)
    else:
        if not match:
            suffix = True
            new_resources.append(target)
            new_resources.append(resource)
        else:
            num_concatenated += 1
if not suffix:
    new_resources.append(target)


datapackage['resources'] = new_resources

all_target_fields = set(fields.keys())


def concatenator(resources):
    for resource_ in resources:
        for row in resource_:
            processed = dict((k, '') for k in all_target_fields)
            values = [(field_mapping[k], v) for (k, v)
                      in row.items()
                      if k in field_mapping]
            assert len(values) > 0
            processed.update(dict(values))
            yield processed


def new_resource_iterator(resource_iterator_):
    while True:
        resource_ = next(resource_iterator_)
        if sources.match(resource_.spec['name']):
            resource_chain = \
                itertools.chain([resource_],
                                itertools.islice(resource_iterator_,
                                                 num_concatenated-1))
            yield concatenator(resource_chain)
        else:
            yield resource_


spew(datapackage, new_resource_iterator(resource_iterator))
