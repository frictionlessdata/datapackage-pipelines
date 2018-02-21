import os
from typing import Iterator

from .base_parser import BaseParser, PipelineSpec


class BasicPipelineParser(BaseParser):

    SPEC_FILENAME = 'pipeline-spec.yaml'

    @classmethod
    def check_filename(cls, filename):
        return filename == cls.SPEC_FILENAME

    @classmethod
    def to_pipeline(cls, spec, fullpath, root_dir='.') -> Iterator[PipelineSpec]:
        dirpath = os.path.dirname(fullpath)

        for pipeline_id, pipeline_details in spec.items():
            pipeline_id = os.path.join(dirpath, pipeline_id)
            pipeline_id = cls.replace_root_dir(pipeline_id, root_dir)
            yield PipelineSpec(path=dirpath,
                               pipeline_id=pipeline_id,
                               pipeline_details=pipeline_details)
