import os
from typing import Iterator  #noqa

import yaml
from datapackage_pipelines.status import status_mgr

from .resolver import resolve_executor
from .errors import SpecError
from .schemas.validator import validate_pipeline
from .parsers import BasicPipelineParser, SourceSpecPipelineParser
from .parsers.base_parser import PipelineSpec
from .hashers import HashCalculator, DependencyMissingException

SPEC_PARSERS = [
    BasicPipelineParser(),
    SourceSpecPipelineParser()
]

YAML_LOADER = yaml.CLoader if 'CLoader' in yaml.__dict__ else yaml.Loader


def resolve_processors(spec: PipelineSpec):
    abspath = os.path.abspath(spec.path)
    for step in spec.pipeline_details['pipeline']:
        if 'executor' not in step:
            step['executor'] = resolve_executor(step,
                                                abspath,
                                                spec.validation_errors)


def process_schedules(spec: PipelineSpec):
    if spec.schedule is None:
        schedule = spec.pipeline_details.get('schedule', {})
        if 'crontab' in schedule:
            schedule = schedule['crontab'].split()
            spec.schedule = schedule


def find_specs(root_dir='.') -> PipelineSpec:
    for dirpath, _, filenames in os.walk(root_dir):
        if dirpath.startswith(os.path.join(root_dir, '.')):
            continue
        for filename in filenames:
            for parser in SPEC_PARSERS:
                if parser.check_filename(filename):
                    fullpath = os.path.join(dirpath, filename)
                    with open(fullpath, encoding='utf8') as spec_file:
                        contents = spec_file.read()
                        try:
                            spec = yaml.load(contents, Loader=YAML_LOADER)
                            yield from parser.to_pipeline(spec, fullpath, root_dir)
                        except yaml.YAMLError as e:
                            error = SpecError('Invalid Spec File %s' % fullpath, str(e))
                            yield PipelineSpec(path=dirpath, validation_errors=[error])


def pipelines(prefixes=None, ignore_missing_deps=False, root_dir='.', status_manager=None):

    specs: Iterator[PipelineSpec] = find_specs(root_dir)
    hasher = HashCalculator()
    if status_manager is None:
        status_manager = status_mgr()
    if prefixes is None:
        prefixes = ('',)
    while specs is not None:
        deferred = []
        found = False

        for spec_ in specs:
            spec: PipelineSpec = spec_

            if not any(spec.pipeline_id.startswith(prefix)
                       for prefix in prefixes):
                continue

            if (spec.pipeline_details is not None and
                    validate_pipeline(spec.pipeline_details, spec.validation_errors)):

                resolve_processors(spec)
                process_schedules(spec)

                try:
                    hasher.calculate_hash(spec, status_manager, ignore_missing_deps)
                    found = True
                except DependencyMissingException as e_:
                    e: DependencyMissingException = e_
                    deferred.append((e.spec, e.missing))
                    continue

            yield spec

        if found and len(deferred) > 0:
            specs = iter((x[0] for x in deferred))
        else:
            for spec, missing in deferred:
                spec.validation_errors.append(
                    SpecError('Missing dependency',
                              'Failed to find a dependency: {}'.format(missing))
                )
                yield spec
            specs = None


def register_all_pipelines(root_dir='.'):
    for spec in pipelines(root_dir=root_dir):
        ps = status_mgr().get(spec.pipeline_id)
        ps.init(spec.pipeline_details,
                spec.source_details,
                spec.validation_errors,
                spec.cache_hash)
        ps.save()
