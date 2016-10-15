import json
import os
import logging
import hashlib

import yaml

from .status import status

SPEC_FILENAME = 'pipeline-spec.yaml'
PROCESSOR_PATH = os.environ.get('DATAPIPELINES_PROCESSOR_PATH', '').split(';')


def find_specs(root_dir='.'):
    for dirpath, _, filenames in os.walk(root_dir):
        if SPEC_FILENAME in filenames:
            abspath = os.path.abspath(dirpath)
            fullpath = os.path.join(abspath, SPEC_FILENAME)
            with open(fullpath, encoding='utf8') as spec_file:
                spec = yaml.load(spec_file.read())
                for pipeline_id, pipeline_details in spec.items():
                    pipeline_id = os.path.join(dirpath, pipeline_id)
                    yield abspath, pipeline_id, pipeline_details


def resolve_executor(executor, path):

    parts = []
    while executor.startswith('..'):
        parts.append('..')
        executor = executor[1:]

    if executor.startswith('.'):
        executor = executor[1:]

    executor = executor.split('.')
    executor[-1] += '.py'

    parts.extend(executor)

    local_parts = [path] + parts
    resolve_order = [local_parts]

    for resolve_location in PROCESSOR_PATH:
        resolve_order.append([os.path.abspath(resolve_location)] + parts)

    lib_parts = [os.path.dirname(__file__), '..', 'lib'] + parts
    resolve_order.append(lib_parts)

    for option in resolve_order:
        option = os.path.join(*option)
        if os.path.exists(option):
            return option

    raise FileNotFoundError("Couldn't resolve {0} at {1}"
                            .format(executor, path))


def validate_required_keys(obj, keys, abspath):
    for required_key in keys:
        if required_key not in obj:
            raise KeyError('Missing parameter {0} in {1}'
                           .format(required_key, abspath))


def validate_specs():

    all_pipeline_ids = set()

    for abspath, pipeline_id, pipeline_details in find_specs():

        if pipeline_id in all_pipeline_ids:
            raise KeyError('Duplicate key {0} in {1}'
                           .format(pipeline_id, abspath))

        validate_required_keys(pipeline_details,
                               ['pipeline', 'schedule'],
                               abspath)

        pipeline = pipeline_details['pipeline']

        try:
            for step in pipeline:
                validate_required_keys(step, ['run'], abspath)
                executor = step['run']
                step['name'] = executor
                step['run'] = resolve_executor(executor, abspath)
        except FileNotFoundError as e:
            logging.error(e)
            continue

        cache_hash = ''
        for step in pipeline:
            m = hashlib.md5()
            m.update(cache_hash.encode('ascii'))
            m.update(open(step['run'], 'rb').read())
            m.update(json.dumps(step, ensure_ascii=True, sort_keys=True)
                     .encode('ascii'))
            cache_hash = m.hexdigest()
            step['_cache_hash'] = cache_hash

        schedule = pipeline_details['schedule']
        if 'crontab' in schedule:
            schedule = schedule['crontab'].split()
            pipeline_details['schedule'] = schedule
        else:
            raise NotImplementedError("Couldn't find valid schedule at {0}"
                                      .format(abspath))

        dirty = status.register(pipeline_id, cache_hash)

        yield pipeline_id, pipeline_details, abspath, dirty


def pipelines():
    return validate_specs()
