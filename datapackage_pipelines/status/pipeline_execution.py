import time


class PipelineExecution(object):

    def __init__(self, backend, pipeline_id, pipeline_details, cache_hash, trigger, execution_id,
                 log='', queue_time=None, start_time=None, finish_time=None, success=None,
                 stats=None, error_log=None,
                 save=True):
        self.backend = backend
        self.pipeline_id = pipeline_id
        self.pipeline_details = pipeline_details
        self.cache_hash = cache_hash
        self.trigger = trigger
        self.execution_id = execution_id
        self.log = log
        self.queue_time = queue_time
        self.start_time = start_time
        self.finish_time = finish_time
        self.success = success
        self.stats = stats or {}
        self.error_log = error_log or []
        if save:
            self.__save()

    @staticmethod
    def from_execution_id(backend, execution_id):
        data = backend.get_status('PipelineExecution:' + execution_id)
        return PipelineExecution(
            backend,
            data['pipeline_id'],
            data['pipeline_details'],
            data['cache_hash'],
            data['trigger'],
            data['execution_id'],
            log=data['log'],
            queue_time=data['queue_time'],
            start_time=data['start_time'],
            finish_time=data['finish_time'],
            success=data['success'],
            stats=data['stats'],
            error_log=data['error_log'],
            save=False
        )

    def __iter__(self):
        yield 'pipeline_id', self.pipeline_id,
        yield 'pipeline_details', self.pipeline_details,
        yield 'cache_hash', self.cache_hash
        yield 'trigger', self.trigger
        yield 'execution_id', self.execution_id
        yield 'log', self.log
        yield 'queue_time', self.queue_time
        yield 'start_time', self.start_time
        yield 'finish_time', self.finish_time
        yield 'success', self.success
        yield 'stats', self.stats
        yield 'error_log', self.error_log

    def __save(self):
        # logging.debug('SAVING PipelineExecution %s/%s -> %r' % (self.pipeline_id, self.execution_id, dict(self)))
        self.backend.set_status('PipelineExecution:' + self.execution_id, dict(self))

    def queue_execution(self, trigger):
        if self.queue_time is not None:
            return False
        self.queue_time = time.time()
        self.trigger = trigger
        self.__save()
        return True

    def start_execution(self):
        if self.queue_time is None or self.start_time is not None:
            return False
        self.start_time = time.time()
        self.__save()
        return True

    def finish_execution(self, success, stats, error_log):
        if self.queue_time is None or self.start_time is None:
            return False
        self.finish_time = time.time()
        self.success = success
        self.stats = stats
        self.error_log = error_log
        self.__save()
        return True

    def update_execution(self, log):
        if self.queue_time is None or self.start_time is None or self.finish_time is not None:
            return False
        self.log = '\n'.join(log)
        self.__save()
        return True

    def invalidate(self):
        now = time.time()
        self.queue_time = self.queue_time or now
        self.start_time = self.start_time or now
        self.finish_time = self.finish_time or now
        self.__save()
        return True

    def delete(self):
        self.backend.del_status(self.execution_id)

    def is_stale(self):
        long_ago = time.time() - 86400  # a day ago
        if self.start_time is not None and self.start_time < long_ago:
            return True
        if self.queue_time is not None and self.queue_time < long_ago:
            return True
