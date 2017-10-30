import unittest.mock as mock
from datapackage_pipelines.wrapper import spew


class TestWrapper(object):
    def test_spew_finalizer_runs_before_we_signal_that_were_done(self):
        '''Assert that the finalizer param is executed before spew is finished.

        We signal to other processors that we're done by writing an empty line
        to STDOUT. The finalizer parameter to spew() must be executed before that,
        as there can be processors that depend on us finishing our processing
        before they're able to run. For example, a processor that depends on
        `dump.to_zip` must wait until it has finished writing to the local
        filesystem.
        '''
        datapackage = {}
        resources_iterator = iter([])

        with mock.patch('sys.stdout') as stdout_mock:
            def finalizer():
                last_call_args = stdout_mock.write.call_args_list[-1]
                assert last_call_args != mock.call('\n')

            spew(datapackage, resources_iterator, finalizer=finalizer)
