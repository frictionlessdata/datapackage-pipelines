import hashlib

from datapackage_pipelines.utilities.extended_json import json

from ..errors import SpecError
from .dependency_resolver import resolve_dependencies


class HashCalculator(object):

    def __init__(self):
        self.all_pipeline_ids = {}

    def calculate_hash(self, spec):

        cache_hash = None
        if spec.pipeline_id in self.all_pipeline_ids:
            message = 'Duplicate key {0} in {1}' \
                .format(spec.pipeline_id, spec.path)
            spec.errors.append(SpecError('Duplicate Pipeline Id', message))

        else:
            cache_hash = resolve_dependencies(spec, self.all_pipeline_ids)
            if len(spec.errors) > 0:
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

            self.all_pipeline_ids[spec.pipeline_id] = spec

        spec.cache_hash = cache_hash
