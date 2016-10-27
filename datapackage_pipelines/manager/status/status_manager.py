import os
import time

from .backend_redis import RedisBackend
from .backend_shelve import ShelveBackend


class StatusManager(object):

    def __init__(self, host=None, port=6379):
        redis = RedisBackend(host, port)
        self.backend = redis if redis.is_init() else ShelveBackend()

    def is_running(self, _id):
        if self.backend is None:
            return False
        _status = self.backend.get_status(_id)
        cur_time = time.time()
        # A running task must have been updated in the last 10 seconds
        is_running = \
            (_status is not None) and \
            (_status['running'] is True) and \
            (cur_time - _status.get('updated', cur_time) < 60)
        return is_running

    def running(self, _id, trigger=None, log=None):
        if self.backend is None:
            return False
        _status = self.backend.get_status(_id)
        if _status is None:
            _status = {}

        cur_time = time.time()
        if not self.is_running(_id):
            _status['started'] = cur_time

        _status.update({
            'id': _id,
            'running': True,
            'trigger': trigger,
            'updated': cur_time,
            'message': 'Running',
            'reason': log
        })
        self.backend.set_status(_id, _status)

    def idle(self, _id, success,
             reason=None,
             cache_hash=None,
             record_count=None):
        if self.backend is None:
            return
        _status = self.backend.get_status(_id)

        cur_time = time.time()
        _status.update({
            'id': _id,
            'running': False,
            'ended': cur_time,
            'updated': cur_time,
            'success': success,
            'message': 'Idle' if success else 'Failed',
            'reason': reason
        })
        if success is True:
            _status.update({
                'last_success': _status['ended'],
                'cache_hash': cache_hash,
                'record_count': record_count
            })
        self.backend.set_status(_id, _status)

    def register(self, _id, cache_hash, pipeline=(), source=None, errors=()):
        if self.backend is None:
            return
        self.backend.register_pipeline_id(_id)
        _status = self.backend.get_status(_id)
        if _status is None:
            _status = {
                'id': _id,
                'cache_hash': ''
            }
        dirty = _status.get('cache_hash') != cache_hash
        _status.update({
            'running': False,
            'pipeline': pipeline,
            'source': source
        })
        if len(errors) > 0:
            _status.update({
                'message': errors[0][0],
                'reason': '\n'.join('{}: {}'.format(*e) for e in errors),
                'success': False
            })
        else:
            _status.update({
                'message': '',
                'reason': '',
                'success': None
            })

        self.backend.set_status(_id, _status)
        return dirty

    def initialize(self):
        if self.backend is None:
            return
        self.backend.reset()

    def get_status(self, _id):
        if self.backend is None:
            return None
        return self.backend.get_status(_id)

    def all_statuses(self):
        if self.backend is None:
            return []
        return self.backend.all_statuses()

status = StatusManager(os.environ.get('DATAPIPELINES_REDIS_HOST'))
