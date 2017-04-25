import logging

import redis

from datapackage_pipelines.utilities.extended_json import json


class RedisBackend(object):

    def __init__(self, host=None, port=6379):
        self.redis = None
        if host is not None and len(host) > 0:
            conn = redis.StrictRedis(host=host, port=port, db=5)
            try:
                conn.ping()
                self.redis = conn
            except redis.exceptions.ConnectionError:
                logging.warning('Failed to connect to Redis, host:%s, port:%s',
                                host, port)
        else:
            logging.info('Skipping redis connection, host:%s, port:%s',
                         host, port)

    def is_init(self):
        return self.redis is not None

    def get_status(self, pipeline_id):
        if self.is_init():
            status = self.redis.get(pipeline_id)
            if status is not None:
                status = json.loads(status.decode('ascii'))
                return status

    def set_status(self, pipeline_id, status):
        if self.is_init():
            self.redis.set(pipeline_id, json.dumps(status, ensure_ascii=True))

    def register_pipeline_id(self, pipeline_id):
        if self.is_init():
            self.redis.sadd('all-pipelines', pipeline_id.strip())

    def deregister_pipeline_id(self, pipeline_id):
        if self.is_init():
            self.redis.srem('all-pipelines', pipeline_id.strip())

    def reset(self):
        if self.is_init():
            self.redis.delete('all-pipelines')

    def all_statuses(self):
        if self.is_init():
            all_ids = self.redis.smembers('all-pipelines')
            pipe = self.redis.pipeline()
            for _id in sorted(all_ids):
                pipe.get(_id)
            return [json.loads(sts.decode('ascii')) for sts in pipe.execute()]
        return []
