import os

from ..resolver import resolve_generator
from ..errors import SpecError
from .base_parser import BaseParser, PipelineSpec


class SourceSpecPipelineParser(BaseParser):

    SOURCE_FILENAME_SUFFIX = '.source-spec.yaml'

    @classmethod
    def check_filename(cls, filename):
        return filename.endswith(cls.SOURCE_FILENAME_SUFFIX)

    @classmethod
    def to_pipeline(cls, source_spec, fullpath, root_dir='.'):
        filename = os.path.basename(fullpath)
        dirpath = os.path.dirname(fullpath)

        module_name = filename[:-len(cls.SOURCE_FILENAME_SUFFIX)]
        pipeline_id = os.path.join(dirpath, module_name)
        generator = resolve_generator(module_name)

        if generator is None:
            message = 'Unknown source description kind "{}" in {}' \
                .format(module_name, fullpath)
            error = SpecError('Unknown source kind', message)
            yield PipelineSpec(pipeline_id=module_name,
                               path=dirpath,
                               validation_errors=[error],
                               pipeline_details={'pipeline': []})
            return

        if generator.internal_validate(source_spec):
            try:
                spec = generator.internal_generate(source_spec)
                for pipeline_id, pipeline_details in spec:
                    if pipeline_id[0] == ':' and pipeline_id[-1] == ':':
                        module = pipeline_id[1:-1]
                        filename = module + cls.SOURCE_FILENAME_SUFFIX
                        yield from cls.to_pipeline(pipeline_details,
                                                   os.path.join(dirpath, filename))
                    else:
                        pipeline_id = os.path.join(dirpath, pipeline_id)
                        pipeline_id = cls.replace_root_dir(pipeline_id, root_dir)
                        for dependency in pipeline_details.get('dependencies', []):
                            if 'pipeline' in dependency:
                                if not dependency['pipeline'].startswith('./'):
                                    dependency['pipeline'] = \
                                        os.path.join(cls.replace_root_dir(dirpath, root_dir),
                                                     dependency['pipeline'])
                        yield PipelineSpec(path=pipeline_details.get('__path', dirpath),
                                           pipeline_id=pipeline_id,
                                           pipeline_details=pipeline_details,
                                           source_details=source_spec)
            except Exception as e:
                message = '"{}" in {}' \
                    .format(e, fullpath)
                error = SpecError('Error converting source', message)
                yield PipelineSpec(pipeline_id=pipeline_id,
                                   path=dirpath, validation_errors=[error],
                                   pipeline_details={'pipeline': []})
        else:
            message = 'Invalid source description for "{}" in {}' \
                .format(module_name, fullpath)
            error = SpecError('Invalid Source', message)
            yield PipelineSpec(pipeline_id=pipeline_id,
                               path=dirpath,
                               validation_errors=[error],
                               pipeline_details={'pipeline': []})
