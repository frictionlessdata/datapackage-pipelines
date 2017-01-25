import json
import sqlite3
import tempfile
import collections

from datapackage_pipelines.wrapper import ingest, spew


# pylint: disable=too-few-public-methods
class KeyCalc(object):

    def __init__(self, key_spec):
        if isinstance(key_spec, list):
            key_spec = ':'.join('{%s}' % key for key in key_spec)
        self.key_spec = key_spec

    def __call__(self, row):
        return self.key_spec.format(**row)


class DB(object):

    def __init__(self):
        self.tmpfile = tempfile.NamedTemporaryFile()
        self.db = sqlite3.connect(self.tmpfile.name)
        self.cursor = self.db.cursor()
        self.cursor.execute('''CREATE TABLE d (key text, value text)''')
        self.cursor.execute('''CREATE UNIQUE INDEX i ON d (key)''')

    def get(self, key):
        ret = self.cursor.execute('''SELECT value FROM d WHERE key=?''',
                                  (key,)).fetchone()
        if ret is None:
            return None
        else:
            return json.loads(ret[0])

    def set(self, key, value):
        value = json.dumps(value)
        if self.get(key) is not None:
            self.cursor.execute('''UPDATE d SET value=? WHERE key=?''',
                                (value, key))
        else:
            self.cursor.execute('''INSERT INTO d VALUES (?, ?)''',
                                (key, value))
        self.db.commit()

db = DB()


def identity(x):
    return x

Aggregator = collections.namedtuple('Aggregator',
                                    ['func', 'finaliser', 'dataType'])
AGGREGATORS = {
    'sum': Aggregator(lambda curr, new:
                      new + curr if curr is not None else new,
                      identity,
                      None),
    'avg': Aggregator(lambda curr, new:
                      (curr[0] + 1, new + curr[1])
                      if curr is not None
                      else (1, new),
                      lambda value: value[1] / value[0],
                      None),
    'max': Aggregator(lambda curr, new:
                      max(new, curr) if curr is not None else new,
                      identity,
                      None),
    'min': Aggregator(lambda curr, new:
                      min(new, curr) if curr is not None else new,
                      identity,
                      None),
    'first': Aggregator(lambda curr, new:
                        curr if curr is not None else new,
                        identity,
                        None),
    'last': Aggregator(lambda curr, new: new,
                       identity,
                       None),
    'count': Aggregator(lambda curr, new:
                        curr+1 if curr is not None else 1,
                        identity,
                        'number'),
    'any': Aggregator(lambda curr, new: new,
                      identity,
                      None),
}

parameters, datapackage, resource_iterator = ingest()

source = parameters['source']
source_name = source['name']
source_key = KeyCalc(source['key'])
source_delete = source.get('delete', False)

target = parameters['target']
target_name = target['name']
target_key = KeyCalc(target['key'])

fields = parameters['fields']
full = parameters.get('full', True)


def get_aggregator(field):
    spec = fields[field]
    agg = spec.get('aggregate', 'any')
    return agg


def indexer(resource):
    for row in resource:
        key = source_key(row)
        current = db.get(key)
        if current is None:
            current = {}
        for field, spec in fields.items():
            curr = current.get(field)
            agg = get_aggregator(field)
            if agg != 'count':
                new = row.get(spec['name'])
            else:
                new = None
            current[field] = AGGREGATORS[agg].func(curr, new)
        db.set(key, current)
        yield row


def process_target(resource):
    empty_extra = dict((f, None) for f in fields.keys())
    for row in resource:
        key = target_key(row)
        extra = db.get(key)
        if extra is None:
            if not full:
                continue
            extra = empty_extra
        extra = dict(
            (k, AGGREGATORS[get_aggregator(k)].finaliser(v))
            for k, v in extra.items()
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
        elif name == target_name:
            assert has_index
            yield process_target(resource)
        else:
            yield resource


def process_datapackage(datapackage_):

    new_resources = []
    source_spec = None
    for resource in datapackage_['resources']:

        if resource['name'] == source_name:
            source_spec = resource
            if source_delete:
                continue

        elif resource['name'] == target_name:
            assert isinstance(source_spec, dict), \
                "Source resource must appear before target resource"
            target_fields = resource['schema']['fields']
            added_fields = sorted(fields.keys())
            for field in added_fields:
                spec = fields[field]
                agg = get_aggregator(field)
                data_type = AGGREGATORS[agg].dataType
                if data_type is None:
                    source_field = \
                        next(filter(lambda f, spec_=spec:
                                    f['name'] == spec_['name'],
                                    source_spec['schema']['fields']))  # pylint: disable=unsubscriptable-object
                    data_type = source_field['type']
                target_fields.append({
                    'name': field,
                    'type': data_type
                })

        new_resources.append(resource)

    datapackage_['resources'] = new_resources
    return datapackage_


spew(process_datapackage(datapackage),
     new_resource_iterator(resource_iterator))
