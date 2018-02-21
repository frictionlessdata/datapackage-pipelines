import os

from .backend_redis import RedisBackend
from .backend_filesystem import FilesystemBackend
from .pipeline_status import PipelineStatus


class StatusManager(object):

    def __init__(self, *, host=None, port=6379, root_dir='.'):
        self._host = host
        self._port = port
        self._backend = None
        self._root_dir = root_dir

    @property
    def backend(self):
        if self._backend is None:
            redis = RedisBackend(self._host, self._port)
            self._backend = redis if redis.is_init() else FilesystemBackend(self._root_dir)
        return self._backend

    def get_errors(self, _id):
        ex = self.get(_id).get_last_execution()
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


_status = None
_root_dir = None


def status_mgr(root_dir='.') -> StatusManager:
    global _status
    global _root_dir

    if _status is not None and _root_dir == root_dir:
        return _status
    _root_dir = root_dir
    _status = StatusManager(host=os.environ.get('DPP_REDIS_HOST'), root_dir=root_dir)
    return _status
