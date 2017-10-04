# -*- coding: utf-8 -*-
import time
from datapackage_pipelines.manager import execute_pipeline
from datapackage_pipelines.specs.specs import pipelines
from datapackage_pipelines.utilities.execution_id import gen_execution_id
from datapackage_pipelines.status import status


def test_pipeline():
    '''Tests a few pipelines.'''
    for spec in pipelines():
        if spec.pipeline_id.startswith('./tests/env/dummy/pipeline-test'):
            eid = gen_execution_id()
            status.get(spec.pipeline_id).queue_execution(eid, 'manual')
            success, _, _ = execute_pipeline(spec, eid, use_cache=False)
            assert success
