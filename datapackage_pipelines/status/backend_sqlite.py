import os
import sqlite3
from datapackage_pipelines.utilities.extended_json import json

DPP_DB_FILENAME = os.environ.get('DPP_DB_FILENAME', '.dpp.db')


class Sqlite3Dict(object):
    def __init__(self, filename):
        self.filename = filename
        conn = sqlite3.connect(self.filename)
        cursor = conn.cursor()
        cursor.execute(
            '''CREATE TABLE IF NOT EXISTS d (_key text, _value text)'''
        )
        conn.commit()
        conn.close()

    def __getitem__(self, key):
        conn = sqlite3.connect(self.filename)
        cursor = conn.cursor()
        result = cursor.execute(
            'SELECT _value from d where _key=?',
            (key,)
        ).fetchone()
        conn.close()
        if result is not None:
            return json.loads(result[0])
        return None

    def __setitem__(self, key, value):
        conn = sqlite3.connect(self.filename)
        value = json.dumps(value)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM d where _key=?', (key,))
        cursor.execute('INSERT INTO d VALUES (?,?)', (key, value))
        conn.commit()
        conn.close()

    def __delitem__(self, key):
        conn = sqlite3.connect(self.filename)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM d where _key=?', (key,))
        conn.commit()
        conn.close()


class SqliteBackend(object):

    KIND = 'sqlite3'
    ALL_PIPELINES_KEY = 'all-pipelines'

    def __init__(self):
        self.db = Sqlite3Dict(DPP_DB_FILENAME)

    def get_status(self, pipeline_id):
        return self.db[pipeline_id]

    def set_status(self, pipeline_id, status):
        self.db[pipeline_id] = status

    def del_status(self, pipeline_id):
        del self.db[pipeline_id]

    def register_pipeline_id(self, pipeline_id):
        all_pipelines = self.db[self.ALL_PIPELINES_KEY]
        if all_pipelines is None:
            all_pipelines = []
        if pipeline_id not in all_pipelines:
            all_pipelines.append(pipeline_id)
        self.db[self.ALL_PIPELINES_KEY] = all_pipelines

    def deregister_pipeline_id(self, pipeline_id):
        all_pipelines = self.db[self.ALL_PIPELINES_KEY]
        if all_pipelines is None:
            all_pipelines = []
        if pipeline_id in all_pipelines:
            all_pipelines = filter(lambda x: x != pipeline_id, all_pipelines)
        self.db[self.ALL_PIPELINES_KEY] = all_pipelines

    def reset(self):
        self.db[self.ALL_PIPELINES_KEY] = []

    def all_pipeline_ids(self):
        all_ids = sorted(self.db[self.ALL_PIPELINES_KEY])
        return all_ids

    def all_statuses(self):
        all_ids = sorted(self.db[self.ALL_PIPELINES_KEY])
        return [self.db[_id] for _id in all_ids]
