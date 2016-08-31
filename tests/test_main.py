# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

from datapackage_pipelines.manager import execute_pipeline
from datapackage_pipelines.manager.specs import pipelines


def test_pipeline():
    '''Tests that what we want for open data is correct.'''
    for pipeline_id, pipeline_details in pipelines():
        print(pipeline_id)
        if pipeline_id == './tests/env/dummy/pipeline-test':
            execute_pipeline(pipeline_id, pipeline_details['pipeline'])
