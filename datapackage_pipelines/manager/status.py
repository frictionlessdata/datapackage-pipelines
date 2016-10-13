import json
import logging
import os
import time

import redis


class RedisConnection(object):

    def __init__(self, host=None, port=6379):
        if host is not None and len(host) > 0:
            self.redis = redis.StrictRedis(host=host, port=port, db=5)
            try:
                self.redis.ping()
            except redis.exceptions.ConnectionError:
                logging.warning('Failed to connect to Redis, host:%s, port:%s',
                                host, port)
                self.redis = None
        else:
            logging.warning('Failed to connect to Redis, host:%s, port:%s',
                            host, port)
            self.redis = None

    def is_running(self, _id):
        if self.redis is None:
            return False
        _status = self.redis.get(_id)
        _status = json.loads(_status.decode('ascii'))
        cur_time = time.time()
        # A running task must have been updated in the last 10 seconds
        is_running = \
            (_status['running'] is True) and \
            (cur_time - _status.get('updated', cur_time) < 60)
        return is_running

    def running(self, _id, trigger=None, log=None):
        if self.redis is None:
            return True
        _status = self.redis.get(_id)
        _status = json.loads(_status.decode('ascii'))

        cur_time = time.time()
        if not self.is_running(_id):
            _status['started'] = cur_time

        _status.update({
            'id': _id,
            'running': True,
            'trigger': trigger,
            'updated': cur_time,
            'reason': log
        })
        self.redis.set(_id, json.dumps(_status))

    def idle(self, _id, success, reason=None, cache_hash=None):
        if self.redis is None:
            return
        _status = self.redis.get(_id)
        _status = json.loads(_status.decode('ascii'))
        cur_time = time.time()
        _status.update({
            'id': _id,
            'running': False,
            'ended': cur_time,
            'updated': cur_time,
            'success': success,
            'reason': reason
        })
        if success is True:
            _status.update({
                'last_success': _status['ended'],
                'cache_hash': cache_hash
            })
        self.redis.set(_id, json.dumps(_status, ensure_ascii=True))

    def register(self, _id, cache_hash):
        if self.redis is None:
            return
        self.redis.sadd('all-pipelines', _id)
        _status = self.redis.get(_id)
        if _status is None:
            _status = {
                'id': _id,
                'cache_hash': ''
            }
        else:
            _status = json.loads(_status.decode('ascii'))
        dirty = _status.get('cache_hash') != cache_hash
        _status.update({
            'running': False,
        })
        self.redis.set(_id, json.dumps(_status, ensure_ascii=True))
        return dirty

    def initialize(self):
        if self.redis is None:
            return
        self.redis.delete('all-pipelines')

    def all_statuses(self):
        if self.redis is None:
            return []
        all_ids = self.redis.smembers('all-pipelines')
        pipe = self.redis.pipeline()
        for _id in all_ids:
            pipe.get(_id)
        return [json.loads(sts.decode('ascii')) for sts in pipe.execute()]

status = RedisConnection(os.environ.get('DATAPIPELINES_REDIS_HOST'))
