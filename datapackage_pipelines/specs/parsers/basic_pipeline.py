import os

import yaml

from ..errors import SpecError
from .base_parser import BaseParser, PipelineSpec


class BasicPipelineParser(BaseParser):

    SPEC_FILENAME = 'pipeline-spec.yaml'

    @classmethod
    def check_filename(cls, filename):
        return filename == cls.SPEC_FILENAME

    @classmethod
    def to_pipeline(cls, fullpath):
        dirpath = os.path.dirname(fullpath)
        with open(fullpath, encoding='utf8') as spec_file:
            try:
                spec = yaml.load(spec_file.read())
                for pipeline_id, pipeline_details in spec.items():
                    pipeline_id = os.path.join(dirpath, pipeline_id)
                    yield PipelineSpec(path=dirpath,
                                       pipeline_id=pipeline_id,
                                       pipeline_details=pipeline_details)
            except yaml.YAMLError as e:
                error = SpecError('Invalid Pipeline Spec', str(e))
                yield PipelineSpec(path=dirpath, errors=[error])
