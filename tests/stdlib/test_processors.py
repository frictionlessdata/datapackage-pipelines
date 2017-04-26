import os
import unittest

import datapackage_pipelines.lib
from datapackage_pipelines.utilities.lib_test_helpers \
    import mock_processor_test

import logging
log = logging.getLogger(__name__)


class TestStandardProcessors(unittest.TestCase):

    def test_add_metadata_processor(self):
        # Input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'hello': 'world'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines.lib.__file__)
        processor_path = os.path.join(processor_dir, 'add_metadata.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = \
            mock_processor_test(processor_path,
                                (params, datapackage, []))

        # Get the returned datapackage and res_iter after calling our processor
        spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        assert spew_dp == {'name': 'my-datapackage', 'project': 'my-project',
                           'resources': [], 'hello': 'world'}
        assert spew_res_iter == []
