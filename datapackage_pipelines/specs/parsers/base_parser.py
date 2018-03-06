class PipelineSpec(object):
    def __init__(self,
                 path=None,
                 pipeline_id=None,
                 pipeline_details=None,
                 source_details=None,
                 validation_errors=None,
                 dependencies=None,
                 cache_hash='',
                 schedule=None):
        self.path = path
        self.pipeline_id = pipeline_id
        self.pipeline_details = pipeline_details
        self.source_details = source_details
        self.validation_errors = [] if validation_errors is None else validation_errors
        self.dependencies = [] if dependencies is None else dependencies
        self.cache_hash = cache_hash
        self.schedule = schedule

    def __str__(self):
        return 'PipelineSpec({}, validation_errors={}, ' \
               'dependencies={}, cache_hash={})'\
            .format(self.pipeline_id, self.validation_errors,
                    self.dependencies, self.cache_hash)

    def __repr__(self):
        return str(self)


class BaseParser(object):

    class InvalidFileException(Exception):
        def __init__(self, short_msg, long_msg):
            self.short_msg = short_msg
            self.long_msg = long_msg

    @classmethod
    def check_filename(cls, filename):
        raise NotImplementedError()

    @classmethod
    def to_pipeline(cls, spec, fullpath):
        raise NotImplementedError()

    @staticmethod
    def replace_root_dir(path, root_dir):
        if root_dir.endswith('/'):
            root_dir = root_dir[:-1]
        if path.startswith(root_dir):
            path = '.' + path[len(root_dir):]
        return path
