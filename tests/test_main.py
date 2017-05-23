# -*- coding: utf-8 -*-

from datapackage_pipelines.manager import execute_pipeline
from datapackage_pipelines.specs.specs import pipelines


def test_pipeline():
    '''Tests a few pipelines.'''
    for spec in pipelines():
        if spec.pipeline_id.startswith('./tests/env/dummy/pipeline-test'):
            success, _ = execute_pipeline(spec, use_cache=False)
            assert success
