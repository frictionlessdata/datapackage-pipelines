import os
import tempfile
import collections
import cachetools
import logging

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.extended_json import json

try:
    import plyvel as DB_ENGINE
    logging.info('Using leveldb for joining')
    db_kind = 'LevelDB'
except ImportError:
    import sqlite3 as DB_ENGINE
    logging.info('Using sqlite for joining')
    db_kind = 'sqlite'


class KeyCalc(object):

    def __init__(self, key_spec):
        if isinstance(key_spec, list):
            key_spec = ':'.join('{%s}' % key for key in key_spec)
        self.key_spec = key_spec

    def __call__(self, row):
        return self.key_spec.format(**row)


class SqliteDB(object):

    def __init__(self):
        self.tmpfile = tempfile.NamedTemporaryFile()
        self.db = DB_ENGINE.connect(self.tmpfile.name)
        self.cursor = self.db.cursor()
        self.cursor.execute('''CREATE TABLE d (key text, value text)''')
        self.cursor.execute('''CREATE UNIQUE INDEX i ON d (key)''')

    def get(self, key):
        ret = self.cursor.execute('''SELECT value FROM d WHERE key=?''',
                                  (key,)).fetchone()
        if ret is None:
            raise KeyError()
        else:
            return json.loads(ret[0])

    def set(self, key, value):
        value = json.dumps(value)
        try:
            self.get(key)
            self.cursor.execute('''UPDATE d SET value=? WHERE key=?''',
                                (value, key))
        except KeyError:
            self.cursor.execute('''INSERT INTO d VALUES (?, ?)''',
                                (key, value))
        self.db.commit()

    def keys(self):
        cursor = self.db.cursor()
        keys = cursor.execute('''SELECT key FROM d ORDER BY key ASC''')
        for key, in keys:
            yield key


class LevelDB(object):

    def __init__(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db = DB_ENGINE.DB(self.tmpdir.name, create_if_missing=True)

    def get(self, key):
        ret = self.db.get(key.encode('utf8'))
        if ret is None:
            raise KeyError()
        else:
            return json.loads(ret.decode('utf8'))

    def set(self, key, value):
        value = json.dumps(value).encode('utf8')
        key = key.encode('utf8')
        self.db.put(key, value)

    def keys(self):
        for key, value in self.db:
            yield key.decode('utf8')


DB = LevelDB if db_kind == 'LevelDB' else SqliteDB


class CachedDB(cachetools.LRUCache):

    def __init__(self):
        super(CachedDB, self).__init__(1024, self._dbget)
        self.db = DB()

    def popitem(self):
        key, value = super(CachedDB, self).popitem()
        self._dbset(key, value)
        return key, value

    def _dbget(self, key):
        value = self.db.get(key)
        return value

    def _dbset(self, key, value):
        assert value is not None
        self.db.set(key, value)

    def sync(self):
        for key in iter(self):
            value = cachetools.Cache.__getitem__(self, key)
            self._dbset(key, value)

    def keys(self):
        self.sync()
        return self.db.keys()


db = CachedDB()


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
                        'integer'),
    'any': Aggregator(lambda curr, new: new,
                      identity,
                      None),
    'set': Aggregator(lambda curr, new:
                      curr.union({new}) if curr is not None else {new},
                      lambda value: list(value) if value is not None else [],
                      'array'),
    'array': Aggregator(lambda curr, new:
                        curr + [new] if curr is not None else [new],
                        identity,
                        'array'),
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
        if data_type is None:
            source_field = \
                next(filter(lambda f, spec_=spec:
                            f['name'] == spec_['name'],
                            source_spec['schema']['fields']))
            data_type = source_field['type']
        try:
            existing_field = next(iter(filter(
                lambda f: f['name'] == field,
                target_fields)))
            assert existing_field['type'] == data_type, \
                'Reusing %s but with different data types: %s != %s' % (field, existing_field['type'], data_type)
        except StopIteration:
            target_fields.append({
                'name': field,
                'type': data_type
            })
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
