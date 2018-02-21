import hashlib

from ...utilities.extended_json import json
from ..parsers.base_parser import PipelineSpec

from ..errors import SpecError
from .dependency_resolver import resolve_dependencies


class HashCalculator(object):

    def __init__(self):
        self.all_pipeline_ids = {}

    def calculate_hash(self, spec: PipelineSpec, status_mgr, ignore_missing_deps=False):

        cache_hash = None
        if spec.pipeline_id in self.all_pipeline_ids:
            message = 'Duplicate key {0} in {1}' \
                .format(spec.pipeline_id, spec.path)
            spec.validation_errors.append(SpecError('Duplicate Pipeline Id', message))

        else:
            if ignore_missing_deps:
                cache_hash = ''
            else:
                cache_hash = resolve_dependencies(spec, self.all_pipeline_ids, status_mgr)

            self.all_pipeline_ids[spec.pipeline_id] = spec
            if len(spec.validation_errors) > 0:
                return cache_hash

            for step in spec.pipeline_details['pipeline']:
                m = hashlib.md5()
                m.update(cache_hash.encode('ascii'))
                with open(step['executor'], 'rb') as f:
                    m.update(f.read())
                m.update(json.dumps(step, ensure_ascii=True, sort_keys=True)
                         .encode('ascii'))
                cache_hash = m.hexdigest()
                step['_cache_hash'] = cache_hash

        spec.cache_hash = cache_hash
