import os

from .backend_redis import RedisBackend
from .backend_sqlite import SqliteBackend
from .pipeline_status import PipelineStatus


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

    def get_errors(self, _id):
        ex = self.get_last_execution(_id)
        if ex is not None:
            return ex.error_log
        return []

    def initialize(self):
        self.backend.reset()

    def get(self, _id) -> PipelineStatus:
        return PipelineStatus(self.backend, _id)

    def all_statuses(self):
        return self.backend.all_statuses()

    def all_pipeline_ids(self):
        return self.backend.all_pipeline_ids()

    def deregister(self, pipeline_id):
        return self.get(pipeline_id).deregister()


status: StatusManager = StatusManager(os.environ.get('DPP_REDIS_HOST'))
