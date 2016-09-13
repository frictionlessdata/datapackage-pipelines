import json
import logging
import os
import time

import redis


class RedisConnection(object):

    def __init__(self, host=None, port=6379):
        if host is not None:
            self.redis = redis.StrictRedis(host=host, port=port, db=5)
        else:
            logging.warning('Failed to connect to Redis, host:%s, port:%s',
                            host, port)
            self.redis = None
        self.initialized = False

    def running(self, _id, trigger=None, log=None):
        if self.redis is None:
            return
        _status = self.redis.get(_id)
        _status = json.loads(_status.decode('ascii'))
        _status.update({
            'id': _id,
            'running': True,
            'started': time.time(),
            'trigger': trigger,
            'reason': log
        })
        self.redis.set(_id, json.dumps(_status))

    def idle(self, _id, success, reason=None):
        if self.redis is None:
            return
        _status = self.redis.get(_id)
        _status = json.loads(_status.decode('ascii'))
        _status.update({
            'id': _id,
            'running': False,
            'ended': time.time(),
            'success': success,
            'reason': reason
        })
        if success is True:
            _status['last_success'] = _status['ended']
        self.redis.set(_id, json.dumps(_status, ensure_ascii=True))

    def register(self, _id):
        if self.redis is None:
            return
        if not self.initialized:
            self.redis.delete('all-pipelines')
            self.initialized = True
        self.redis.sadd('all-pipelines', _id)
        if self.redis.get(_id) is None:
            _status = {
                'id': _id,
                'running': False
            }
            self.redis.set(_id, json.dumps(_status, ensure_ascii=True))

    def all_statuses(self):
        if self.redis is None:
            return []
        all_ids = self.redis.smembers('all-pipelines')
        pipe = self.redis.pipeline()
        for _id in all_ids:
            pipe.get(_id)
        return [json.loads(sts.decode('ascii')) for sts in pipe.execute()]

status = RedisConnection(os.environ.get('DATAPIPELINES_REDIS_HOST'))
