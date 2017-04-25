import os

import yaml

from ..resolver import resolve_generator
from ..errors import SpecError
from .base_parser import BaseParser, PipelineSpec


class SourceSpecPipelineParser(BaseParser):

    SOURCE_FILENAME_SUFFIX = '.source-spec.yaml'

    @classmethod
    def check_filename(cls, filename):
        return filename.endswith(cls.SOURCE_FILENAME_SUFFIX)

    @classmethod
    def to_pipeline(cls, fullpath):
        filename = os.path.basename(fullpath)
        dirpath = os.path.dirname(fullpath)

        module_name = filename[:-len(cls.SOURCE_FILENAME_SUFFIX)]
        generator = resolve_generator(module_name)

        if generator is None:
            message = 'Unknown source description kind "{}" in {}' \
                .format(module_name, fullpath)
            error = SpecError('Unknown source kind', message)
            yield PipelineSpec(pipeline_id=module_name, path=dirpath, errors=[error])
            return

        try:
            with open(fullpath, encoding='utf8') as spec_file:
                source_spec = yaml.load(spec_file.read())
                if generator.internal_validate(source_spec):
                    spec = generator.internal_generate(source_spec)
                    for pipeline_id, pipeline_details in spec:
                        pipeline_id = os.path.join(dirpath, pipeline_id)
                        yield PipelineSpec(path=pipeline_details.get('__path', dirpath),
                                           pipeline_id=pipeline_id,
                                           pipeline_details=pipeline_details,
                                           source_details=source_spec)
                else:
                    message = 'Invalid source description for "{}" in {}' \
                        .format(module_name, fullpath)
                    error = SpecError('Invalid Source', message)
                    yield PipelineSpec(path=dirpath, errors=[error])

        except yaml.YAMLError as e:
            error = SpecError('Invalid Source Spec', str(e))
            yield PipelineSpec(dirpath, None, None, None, [error])
