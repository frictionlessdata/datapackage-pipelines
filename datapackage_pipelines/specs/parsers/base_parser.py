class PipelineSpec(object):
    def __init__(self,
                 path=None,
                 pipeline_id=None,
                 pipeline_details=None,
                 source_details=None,
                 errors=None,
                 dependencies=None,
                 dirty=None,
                 cache_hash='',
                 schedule=None):
        self.path = path
        self.pipeline_id = pipeline_id
        self.pipeline_details = pipeline_details
        self.source_details = source_details
        self.errors = [] if errors is None else errors
        self.dependencies = [] if dependencies is None else dependencies
        self.dirty = dirty
        self.cache_hash = cache_hash
        self.schedule = schedule

    def __str__(self):
        return 'PipelineSpec({}, dirty={}, errors={}, ' \
               'dependencies={}, cache_hash={})'\
            .format(self.pipeline_id, self.dirty, self.errors,
                    self.dependencies, self.cache_hash)


class BaseParser(object):

    class InvalidFileException(Exception):
        def __init__(self, short_msg, long_msg):
            self.short_msg = short_msg
            self.long_msg = long_msg

    @classmethod
    def check_filename(cls, filename):
        raise NotImplementedError()

    @classmethod
    def to_pipeline(cls, fullpath):
        raise NotImplementedError()
