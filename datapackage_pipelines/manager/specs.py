import os
import hashlib

import yaml
import datapackage
from datapackage.exceptions import DataPackageException

from ..utilities.extended_json import json
from .resolver import resolve_executor, resolve_generator
from .status import status

SPEC_FILENAME = 'pipeline-spec.yaml'
SOURCE_FILENAME_SUFFIX = '.source-spec.yaml'


def find_pipeline_specs(dirpath, filenames):
    if SPEC_FILENAME in filenames:
        abspath = os.path.abspath(dirpath)
        fullpath = os.path.join(abspath, SPEC_FILENAME)
        try:
            with open(fullpath, encoding='utf8') as spec_file:
                spec = yaml.load(spec_file.read())
                for pipeline_id, pipeline_details in spec.items():
                    pipeline_id = os.path.join(dirpath, pipeline_id)
                    yield abspath, pipeline_id, pipeline_details, None, []
        except yaml.YAMLError as e:
            yield abspath, dirpath, {}, None, \
                  [('Invalid Pipeline Spec', str(e))]


# pylint: disable=too-many-locals
def find_source_specs(dirpath, filenames):
    abspath = os.path.abspath(dirpath)
    for filename in filenames:
        if not filename.endswith(SOURCE_FILENAME_SUFFIX):
            continue

        fullpath = os.path.join(abspath, filename)

        module_name = filename[:-len(SOURCE_FILENAME_SUFFIX)]
        generator = resolve_generator(module_name)
        if generator is None:
            message = 'Unknown source description kind "{}" in {}'\
                      .format(module_name, fullpath)
            yield abspath, dirpath, {}, None, \
                  [('Unknown source kind', message)]
            continue

        try:
            with open(fullpath, encoding='utf8') as spec_file:
                source_spec = yaml.load(spec_file.read())
                if generator.internal_validate(source_spec):
                    spec = generator.internal_generate(source_spec)
                    for pipeline_id, schedule, steps in spec:
                        pipeline_details = {
                            'schedule': {'crontab': schedule},
                            'pipeline': steps
                        }
                        pipeline_id = os.path.join(dirpath, pipeline_id)
                        yield abspath, pipeline_id, \
                              pipeline_details, source_spec, []
                else:
                    message = 'Invalid source description for "{}" in {}'\
                              .format(module_name, fullpath)
                    yield abspath, dirpath, {}, \
                          None, [('Invalid Source', message)]
        except yaml.YAMLError as e:
            yield abspath, dirpath, {}, None, [('Invalid Source Spec', str(e))]


def find_specs(root_dir='.'):
    for dirpath, _, filenames in os.walk(root_dir):

        # Pipeline Specs
        for p in find_pipeline_specs(dirpath, filenames):
            yield p

        # Source specs:
        for p in find_source_specs(dirpath, filenames):
            yield p


def validate_required_keys(obj, keys, abspath, errors):
    for required_key in keys:
        if required_key not in obj:
            message = 'Missing parameter {0} in {1}'\
                      .format(required_key, abspath)
            errors.append(('Missing Parameter', message))


def resolve_dependencies(dependencies, all_pipeline_ids):
    errors = []
    cache_hash = ''
    for dependency in dependencies:
        if 'pipeline' in dependency:
            pipeline_id = dependency['pipeline']
            if pipeline_id not in all_pipeline_ids:
                return None, None  # Defer!
            dirty = all_pipeline_ids.get(pipeline_id)['_dirty']
            if dirty:
                errors.append(
                    ('Dirty dependency',
                     'Cannot run until all dependencies are executed')
                )
            pipeline_hash = all_pipeline_ids.get(pipeline_id)['_cache_hash']
            cache_hash += pipeline_hash

        elif 'datapackage' in dependency:
            dp_id = dependency['datapackage']
            try:
                dp = datapackage.DataPackage(dp_id)
                if 'hash' in dp.descriptor:
                    cache_hash += dp.descriptor['hash']
                else:
                    errors.append(('Missing dependency',
                                   "Couldn't get data from datapackage %s"
                                   % dp_id))
            except DataPackageException:
                errors.append(('Missing dependency',
                               "Couldn't open datapackage %s"
                               % dp_id))

        else:
            errors.append(('Missing dependency',
                           'Unknown dependency provided (%r)' % dependency))
    return cache_hash, errors


def calculate_hash(dependencies, pipeline, all_pipeline_ids):
    cache_hash, errors = resolve_dependencies(dependencies, all_pipeline_ids)
    if errors is None:
        return None, None  # Defer!
    elif len(errors) > 0:
        return cache_hash, errors
    for step in pipeline:
        m = hashlib.md5()
        m.update(cache_hash.encode('ascii'))
        m.update(open(step['run'], 'rb').read())
        m.update(json.dumps(step, ensure_ascii=True, sort_keys=True)
                 .encode('ascii'))
        cache_hash = m.hexdigest()
        step['_cache_hash'] = cache_hash
    return cache_hash, errors


# pylint: disable=too-many-locals, too-many-branches
def validate_specs():

    all_pipeline_ids = {}

    specs = find_specs()
    deferred_amount = 0
    while specs is not None:
        deferred = []

        for spec in specs:

            abspath, pipeline_id, pipeline_details, \
                source_details, errors = spec

            if pipeline_id in all_pipeline_ids:
                message = 'Duplicate key {0} in {1}'\
                          .format(pipeline_id, abspath)
                errors.append(('Duplicate Pipeline Id', message))

            validate_required_keys(pipeline_details,
                                   ['pipeline'],
                                   abspath,
                                   errors)

            pipeline = pipeline_details.get('pipeline', [])
            dependencies = pipeline_details.get('dependencies', [])

            try:
                for step in pipeline:
                    if 'name' not in step:
                        validate_required_keys(step, ['run'], abspath, errors)
                        executor = step['run']
                        step['name'] = executor
                        step['run'] = resolve_executor(executor, abspath)
            except FileNotFoundError as e:
                errors.append(('Unresolved processor', str(e)))

            cache_hash = ''
            if len(errors) == 0:
                cache_hash, hash_errors = \
                    calculate_hash(dependencies, pipeline, all_pipeline_ids)
                if hash_errors is None:
                    deferred.append(spec)
                    continue
                errors.extend(hash_errors)

            all_pipeline_ids[pipeline_id] = pipeline_details

            schedule = pipeline_details.get('schedule', {})
            if 'crontab' in schedule:
                schedule = schedule['crontab'].split()
                pipeline_details['schedule'] = schedule

            dirty = status.register(pipeline_id, cache_hash,
                                    pipeline=pipeline_details,
                                    source=source_details,
                                    errors=errors)

            pipeline_details['_cache_hash'] = cache_hash
            pipeline_details['_dirty'] = dirty

            yield pipeline_id, pipeline_details, abspath, dirty, errors

        if len(deferred) > 0:
            if len(deferred) == deferred_amount:
                for spec in deferred:
                    spec[-1].append(('Missing dependency',
                                     'Failed to find pipeline dependency'))
            deferred_amount = len(deferred)
            specs = iter(deferred)
        else:
            specs = None


def pipelines():
    return validate_specs()
