import logging
import os
import time

from .backend_redis import RedisBackend
from .backend_sqlite import SqliteBackend


class PipelineStatus(object):

    STATES = {
        'INIT': {},
        'REGISTERED': {
            'success': None,
            'message': "Didn't run",
            'reason': "Didn't run yet"
        },
        'INVALID': {
            'success': False,
        },
        'RUNNING': {
            'success': None,
            'message': 'Running',
        },
        'SUCCEEDED': {
            'success': True,
            'message': 'Succeeded',
        },
        'FAILED': {
            'success': False,
            'message': 'Failed'
        }
    }

    def __init__(self, backend, pipeline_id):
        self.backend = backend
        self.pipeline_id = pipeline_id
        self.data = backend.get_status(pipeline_id)
        if self.data is None or 'state' not in self.data:
            self.data = {'state': 'INIT'}
        self.data.update({
            'id': self.pipeline_id,
        })

    def set_state(self, state):
        self.data['state'] = state
        self.data['dirty'] = False
        self.data.update(self.STATES[state])

    def check_running(self):
        cur_time = time.time()
        if self.data is None:
            return False

        # A running task must have been updated in the last 10 seconds
        running = self.data.get('state') == 'RUNNING'
        updating = (cur_time - self.data.get('updated', 0)) < 60
        if running and not updating:
            self.set_idle(False, force=True)
        return self.data.get('state') == 'RUNNING'

    def check_successful(self):
        return self.data.get('state') == 'SUCCEEDED'

    def check_waiting(self):
        return self.data.get('state') in {'REGISTERED', 'INVALID'}

    def set_running(self, trigger, log):
        if self.data['state'] not in {'REGISTERED',
                                      'SUCCEEDED',
                                      'FAILED',
                                      'RUNNING'}:
            logging.error('set_running: bad state %s', self.data['state'])
            return

        cur_time = time.time()
        if not self.check_running():
            self.data.update({
                'started': cur_time,
                'trigger': trigger,
            })
            self.set_state('RUNNING')

        self.data.update({
            'reason': log
        })
        self.save()

    def set_idle(self, success,
                 log=None, cache_hash=None, stats=None, force=False):
        if self.data['state'] not in {'RUNNING'}:
            logging.error('set_idle: bad state %s', self.data['state'])
            return

        if stats is None:
            stats = {}

        cur_time = time.time()
        if force or self.check_running():
            self.data.update({
                'ended': cur_time,
                'reason': log,
            })
            if success is True:
                self.data.update({
                    'last_success': self.data['ended'],
                    'cache_hash': cache_hash,
                    'stats': stats,
                })
                self.set_state('SUCCEEDED')
            else:
                self.set_state('FAILED')
        self.save()

    def set_queued(self):
        cur_time = time.time()
        self.data.update({
            'queued': cur_time,
        })
        self.save()

    def register(self, cache_hash, pipeline, source, errors):

        if self.check_running():
            return False

        # Is pipeline dirty?
        dirty = self.data.setdefault('cache_hash', '') != cache_hash
        dirty = dirty or self.data.get('state') != 'SUCCEEDED'
        dirty = dirty and len(errors) == 0

        self.data.update({
            'pipeline': pipeline,
            'source': source,
            'dirty': False
        })
        if len(errors) > 0:
            self.data.update({
                'message': errors[0][0],
                'reason': '\n'.join('{}: {}'.format(*e) for e in errors),
            })
            self.set_state('INVALID')
        else:
            if self.data.get('state') in {'INIT', 'INVALID'}:
                self.set_state('REGISTERED')
            self.data['dirty'] = dirty

        self.backend.register_pipeline_id(self.pipeline_id)
        self.save()
        return dirty

    def deregister(self):
        self.backend.deregister_pipeline_id(self.pipeline_id)

    def save(self):
        cur_time = time.time()
        self.data.update({
            'updated': cur_time
        })
        self.backend.set_status(self.pipeline_id, self.data)


class StatusManager(object):

    def __init__(self, host=None, port=6379):
        self._host = host
        self._port = port
        self._backend = None

    @property
    def backend(self):
        if self._backend is None:
            redis = RedisBackend(self._host, self._port)
            self._backend = redis if redis.is_init() else SqliteBackend()
        return self._backend

    def is_running(self, _id):
        return PipelineStatus(self.backend, _id).check_running()

    def is_waiting(self, _id):
        return PipelineStatus(self.backend, _id).check_waiting()

    def is_successful(self, _id):
        return PipelineStatus(self.backend, _id).check_successful()

    def running(self, _id, trigger=None, log=None):
        PipelineStatus(self.backend, _id).set_running(trigger, log)

    def idle(self, _id, success, reason, cache_hash, stats):
        PipelineStatus(self.backend, _id) \
            .set_idle(success, reason, cache_hash, stats)

    def register(self, _id, cache_hash, pipeline=(), source=None, errors=()):
        return PipelineStatus(self.backend, _id) \
            .register(cache_hash, pipeline, source, errors)

    def deregister(self, _id):
        return PipelineStatus(self.backend, _id) \
            .deregister()

    def queued(self, _id):
        ps = PipelineStatus(self.backend, _id)
        ps.set_queued()
        return ps

    def initialize(self):
        self.backend.reset()

    def get_status(self, _id):
        return self.backend.get_status(_id)

    def all_statuses(self):
        return self.backend.all_statuses()


status = StatusManager(os.environ.get('DPP_REDIS_HOST'))
