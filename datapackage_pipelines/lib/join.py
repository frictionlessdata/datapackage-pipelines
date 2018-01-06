import copy
import os
import collections

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.kvstore import KVStore
from datapackage_pipelines.utilities.resources import PROP_STREAMING

db = KVStore()


class KeyCalc(object):

    def __init__(self, key_spec):
        if isinstance(key_spec, list):
            key_spec = ':'.join('{%s}' % key for key in key_spec)
        self.key_spec = key_spec

    def __call__(self, row):
        return self.key_spec.format(**row)


def identity(x):
    return x


Aggregator = collections.namedtuple('Aggregator',
                                    ['func', 'finaliser', 'dataType', 'copyProperties'])
AGGREGATORS = {
    'sum': Aggregator(lambda curr, new:
                      new + curr if curr is not None else new,
                      identity,
                      None,
                      False),
    'avg': Aggregator(lambda curr, new:
                      (curr[0] + 1, new + curr[1])
                      if curr is not None
                      else (1, new),
                      lambda value: value[1] / value[0],
                      None,
                      False),
    'max': Aggregator(lambda curr, new:
                      max(new, curr) if curr is not None else new,
                      identity,
                      None,
                      False),
    'min': Aggregator(lambda curr, new:
                      min(new, curr) if curr is not None else new,
                      identity,
                      None,
                      False),
    'first': Aggregator(lambda curr, new:
                        curr if curr is not None else new,
                        identity,
                        None,
                        True),
    'last': Aggregator(lambda curr, new: new,
                       identity,
                       None,
                       True),
    'count': Aggregator(lambda curr, new:
                        curr+1 if curr is not None else 1,
                        identity,
                        'integer',
                        False),
    'any': Aggregator(lambda curr, new: new,
                      identity,
                      None,
                      True),
    'set': Aggregator(lambda curr, new:
                      curr.union({new}) if curr is not None else {new},
                      lambda value: list(value) if value is not None else [],
                      'array',
                      False),
    'array': Aggregator(lambda curr, new:
                        curr + [new] if curr is not None else [new],
                        lambda value: value if value is not None else [],
                        'array',
                        False),
}

parameters, datapackage, resource_iterator = ingest()

source = parameters['source']
source_name = source['name']
source_key = KeyCalc(source['key'])
source_delete = source.get('delete', False)

target = parameters['target']
target_name = target['name']
if target['key'] is not None:
    target_key = KeyCalc(target['key'])
else:
    target_key = None
deduplication = target_key is None


def fix_fields(fields_):
    for field in sorted(fields_.keys()):
        spec = fields_[field]
        if spec is None:
            fields_[field] = spec = {}
        if 'name' not in spec:
            spec['name'] = field
        if 'aggregate' not in spec:
            spec['aggregate'] = 'any'
    return fields_


fields = fix_fields(parameters['fields'])
full = parameters.get('full', True)


def indexer(resource):
    for row in resource:
        key = source_key(row)
        try:
            current = db[key]
        except KeyError:
            current = {}
        for field, spec in fields.items():
            name = spec['name']
            curr = current.get(field)
            agg = spec['aggregate']
            if agg != 'count':
                new = row.get(name)
            else:
                new = ''
            if new is not None:
                current[field] = AGGREGATORS[agg].func(curr, new)
            elif field not in current:
                current[field] = None
        db[key] = current
        yield row


def process_target(resource):
    if deduplication:
        # just empty the iterable
        collections.deque(indexer(resource), maxlen=0)
        for key in db.keys():
            row = dict(
                (f, None) for f in fields.keys()
            )
            row.update(dict(
                (k, AGGREGATORS[fields[k]['aggregate']].finaliser(v))
                for k, v in db.db.get(key).items()
            ))
            yield row
    else:
        for row in resource:
            key = target_key(row)
            try:
                extra = db[key]
                extra = dict(
                    (k, AGGREGATORS[fields[k]['aggregate']].finaliser(v))
                    for k, v in extra.items()
                )
            except KeyError:
                if not full:
                    continue
                extra = dict(
                    (k, row.get(k))
                    for k in fields.keys()
                )
            row.update(extra)
            yield row


def new_resource_iterator(resource_iterator_):
    has_index = False
    for resource in resource_iterator_:
        name = resource.spec['name']
        if name == source_name:
            has_index = True
            if source_delete:
                # just empty the iterable
                collections.deque(indexer(resource), maxlen=0)
            else:
                yield indexer(resource)
            if deduplication:
                yield process_target(resource)
        elif name == target_name:
            assert has_index
            yield process_target(resource)
        else:
            yield resource


def process_target_resource(source_spec, resource):
    target_fields = \
        resource.setdefault('schema', {}).setdefault('fields', [])
    added_fields = sorted(fields.keys())
    for field in added_fields:
        spec = fields[field]
        agg = spec['aggregate']
        data_type = AGGREGATORS[agg].dataType
        copy_properties = AGGREGATORS[agg].copyProperties
        to_copy = {}
        if data_type is None:
            source_field = \
                next(filter(lambda f, spec_=spec:
                            f['name'] == spec_['name'],
                            source_spec['schema']['fields']))
            if copy_properties:
                to_copy = copy.deepcopy(source_field)
            data_type = source_field['type']
        try:
            existing_field = next(iter(filter(
                lambda f: f['name'] == field,
                target_fields)))
            assert existing_field['type'] == data_type, \
                'Reusing %s but with different data types: %s != %s' % (field, existing_field['type'], data_type)
        except StopIteration:
            to_copy.update({
                'name': field,
                'type': data_type
            })
            target_fields.append(to_copy)
    return resource


def process_datapackage(datapackage_):

    new_resources = []
    source_spec = None

    for resource in datapackage_['resources']:

        if resource['name'] == source_name:
            source_spec = resource
            if not source_delete:
                new_resources.append(resource)
            if deduplication:
                resource = process_target_resource(
                    source_spec,
                    {
                        'name': target_name,
                        PROP_STREAMING: True,
                        'path': os.path.join('data', target_name + '.csv')
                    })
                new_resources.append(resource)

        elif resource['name'] == target_name:
            assert isinstance(source_spec, dict), \
                "Source resource must appear before target resource"
            resource = process_target_resource(source_spec, resource)
            new_resources.append(resource)

        else:
            new_resources.append(resource)

    datapackage_['resources'] = new_resources
    return datapackage_


spew(process_datapackage(datapackage),
     new_resource_iterator(resource_iterator))
