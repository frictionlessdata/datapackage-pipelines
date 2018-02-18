import cachetools
import tempfile
import logging

from .extended_json import json

try:
    import plyvel as DB_ENGINE
    logging.info('Using leveldb for joining')
    db_kind = 'LevelDB'
except ImportError:
    import sqlite3 as DB_ENGINE
    logging.info('Using sqlite for joining')
    db_kind = 'sqlite'


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

    def items(self):
        cursor = self.db.cursor()
        items = cursor.execute('''SELECT key, value FROM d ORDER BY key ASC''')
        for key, value in items:
            yield key, json.loads(value)


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

    def items(self):
        for key, value in self.db:
            yield (key.decode('utf8'), json.loads(value.decode('utf8')))


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


KVStore = CachedDB
