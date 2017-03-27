# -*- coding: utf-8 -*-

from datapackage_pipelines.manager import execute_pipeline
from datapackage_pipelines.specs.specs import pipelines


def test_pipeline():
    '''Tests that what we want for open data is correct.'''
    for spec in pipelines():
        if spec.pipeline_id == './tests/env/dummy/pipeline-test':
            execute_pipeline(spec.pipeline_id,
                             spec.pipeline_details['pipeline'],
                             spec.path,
                             use_cache=False)
