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
        self.initialized = False

    def running(self, _id, trigger=None, log=None):
        if self.redis is None:
            return True
        _status = self.redis.get(_id)
        _status = json.loads(_status.decode('ascii'))
        cur_time = time.time()
        # If a task was interrupted, after a minute it implicitly becomes idle
        was_idle = \
            (_status['running'] is False) or \
            (_status.get('updated', cur_time) - cur_time > 60)
        if was_idle:
            _status['started'] = cur_time
        _status.update({
            'id': _id,
            'running': True,
            'trigger': trigger,
            'updated': cur_time,
            'reason': log
        })
        self.redis.set(_id, json.dumps(_status))
        return was_idle

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
        if not self.initialized:
            self.redis.delete('all-pipelines')
            self.initialized = True
        self.redis.sadd('all-pipelines', _id)
        _status = self.redis.get(_id)
        _status = json.loads(_status.decode('ascii'))
        if _status is None:
            _status = {
                'id': _id,
                'cache_hash': ''
            }
        dirty = _status['cache_hash'] != cache_hash
        _status.update({
            'updated': time.time(),
            'running': False,
        })
        self.redis.set(_id, json.dumps(_status, ensure_ascii=True))
        return dirty

    def all_statuses(self):
        if self.redis is None:
            return []
        all_ids = self.redis.smembers('all-pipelines')
        pipe = self.redis.pipeline()
        for _id in all_ids:
            pipe.get(_id)
        return [json.loads(sts.decode('ascii')) for sts in pipe.execute()]

status = RedisConnection(os.environ.get('DATAPIPELINES_REDIS_HOST'))
