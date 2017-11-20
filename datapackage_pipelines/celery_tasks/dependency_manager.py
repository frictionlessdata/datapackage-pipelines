import logging
import os

import redis


class DependencyManager(object):

    def __init__(self, host=os.environ.get('DPP_REDIS_HOST'), port=6379):
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

    @staticmethod
    def dependents_key(x):
        return 'Dependents:%s' % x

    @staticmethod
    def dependencies_key(x):
        return 'Dependencies:%s' % x

    @staticmethod
    def encode(x):
        if isinstance(x, str):
            return x.encode('utf8')
        if isinstance(x, list):
            return [y.encode('utf8') for y in x]

    @staticmethod
    def decode(x):
        if isinstance(x, bytes):
            return x.decode('utf8')
        if isinstance(x, (list, set)):
            return [y.decode('utf8') for y in x]
        assert False, "Unknown type for x: %r" % x

    def is_init(self):
        return self.redis is not None

    def update(self, spec):
        if self.is_init():
            for dep in spec.dependencies:
                self.redis.sadd(self.dependents_key(dep), self.encode(spec.pipeline_id))
            self.redis.delete(self.dependencies_key(spec.pipeline_id))
            for dep in self.encode(spec.dependencies):
                self.redis.sadd(self.dependencies_key(spec.pipeline_id), dep)

    def get_dependencies(self, pipeline_id):
        if self.is_init():
            members = self.redis.smembers(self.dependencies_key(pipeline_id))
            if members is not None:
                return self.decode(members)
        return []

    def get_dependents(self, pipeline_id):
        if self.is_init():
            members = self.redis.smembers(self.dependents_key(pipeline_id))
            if members is not None:
                return self.decode(members)
        return []

    def remove(self, pipeline_id):
        if self.is_init():
            dependencies = self.get_dependencies(pipeline_id)
            dependents = self.get_dependents(pipeline_id)

            for p in dependencies:
                self.redis.srem(self.dependents_key(p), self.encode(pipeline_id))
            for p in dependents:
                self.redis.srem(self.dependencies_key(p), self.encode(pipeline_id))
            self.redis.delete(self.dependents_key(pipeline_id))
            self.redis.delete(self.dependencies_key(pipeline_id))
