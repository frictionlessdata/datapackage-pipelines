import os

from datapackage_pipelines.status import status

from .resolver import resolve_executor
from .errors import SpecError
from .schemas.validator import validate_pipeline
from .parsers import BasicPipelineParser, SourceSpecPipelineParser
from .hashers import HashCalculator, DependencyMissingException

SPEC_PARSERS = [
    BasicPipelineParser(),
    SourceSpecPipelineParser()
]


def resolve_processors(spec):
    abspath = os.path.abspath(spec.path)
    for step in spec.pipeline_details['pipeline']:
        if 'executor' not in step:
            step['executor'] = resolve_executor(step,
                                                abspath,
                                                spec.errors)


def process_schedules(spec):
    if spec.schedule is None:
        schedule = spec.pipeline_details.get('schedule', {})
        if 'crontab' in schedule:
            schedule = schedule['crontab'].split()
            spec.schedule = schedule


def calculate_dirty(spec):
    pipeline_status = status.get_status(spec.pipeline_id) or {}
    dirty = pipeline_status.get('cache_hash', '') != spec.cache_hash
    dirty = dirty or pipeline_status.get('state') != 'SUCCEEDED'
    dirty = dirty and len(spec.errors) == 0

    spec.dirty = dirty


def find_specs(root_dir='.'):
    for dirpath, _, filenames in os.walk(root_dir):
        if dirpath.startswith('./.'):
            continue
        for filename in filenames:
            for parser in SPEC_PARSERS:
                if parser.check_filename(filename):
                    yield from parser.to_pipeline(os.path.join(dirpath, filename))


def pipelines():

    specs = find_specs()
    hasher = HashCalculator()
    deferred_amount = 0
    while specs is not None:
        deferred = []

        for spec in specs:
            if (spec.pipeline_details is not None and
                    validate_pipeline(spec.pipeline_details, spec.errors)):

                resolve_processors(spec)
                process_schedules(spec)

                if len(spec.errors) == 0:
                    try:
                        hasher.calculate_hash(spec)
                    except DependencyMissingException:
                        deferred.append(spec)
                        continue

                calculate_dirty(spec)

            yield spec

        if len(deferred) > 0:
            if len(deferred) == deferred_amount:
                for spec in deferred:
                    spec.errors.append(
                        SpecError('Missing dependency',
                                  'Failed to find a pipeline dependency')
                    )
            deferred_amount = len(deferred)
            specs = iter(deferred)
        else:
            specs = None


def register_all_pipelines():
    for spec in pipelines():
        status.register(spec.pipeline_id,
                        spec.cache_hash,
                        pipeline=spec.pipeline_details,
                        source=spec.source_details,
                        errors=spec.errors)
