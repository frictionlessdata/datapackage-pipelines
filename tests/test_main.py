# -*- coding: utf-8 -*-
import os

from datapackage_pipelines.manager import execute_pipeline
from datapackage_pipelines.manager.specs import pipelines

def test_pipeline():
    '''Tests that what we want for open data is correct.'''
    for pipeline_id, pipeline_details, pipeline_cwd, _, _ in pipelines():
        if pipeline_id == './tests/env/dummy/pipeline-test':
            execute_pipeline(pipeline_id, pipeline_details['pipeline'], pipeline_cwd,
                             use_cache=False)
