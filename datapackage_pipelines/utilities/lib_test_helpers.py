"""
This module contains code that can be used to run automated tests of
datapackage pipelines processors

It is used internally to test the standard library processors but supports
external use as well.
"""
import os
import subprocess
import sys
import json
import mock
import importlib


class ProcessorFixtureTestsBase(object):

    def __init__(self, fixtures_path):
        self._fixtures_path = fixtures_path

    def get_tests(self):
        for dirpath, _, filenames in os.walk(self._fixtures_path):
            for filename in filenames:
                data_in, data_out, dp_out, params, processor = \
                    self._load_fixture(dirpath, filename)

                def inner(processor_, params_, data_in_, dp_out_, data_out_):
                    def inner2():
                        return self._test_single_fixture(
                            processor_, params_,
                            data_in_, dp_out_,
                            data_out_,
                            self._get_procesor_env())
                    return inner2
                yield filename, inner(processor, params,
                                      data_in, dp_out, data_out)

    def _get_procesor_env(self):
        """
        Set the environment variables for the sub-process that runs the
        processor extending classes will most likely want to set the PYTHONPATH
        variable to correct value
        """
        return {}

    def _get_processor_file(self, processor):
        """
        Must be implemented by extending classes to return the path to the
        processor python file.

        :param processor: the value of the 1st part of the fixture (the name of
            the processor to test)
        :return: full path to the processor python file
        """
        raise NotImplementedError()

    def _load_fixture(self, dirpath, filename):
        with open(os.path.join(dirpath, filename), encoding='utf8') as f:
            parts = f.read().split('--\n')
        processor, params, dp_in, data_in, dp_out, data_out = parts
        processor_file = self._get_processor_file(processor)
        params = rejsonize(params)
        dp_out = rejsonize(dp_out)
        dp_in = rejsonize(dp_in)
        data_in = (dp_in + '\n\n' + data_in).encode('utf8')
        return data_in, data_out, dp_out, params, processor_file

    def _run_processor(self, processor, parameters, data_in, env):
        '''Run the passed `processor` and return the output'''
        process = subprocess.run([sys.executable, processor, '1',
                                  parameters, 'False', ''],
                                 input=data_in,
                                 stdout=subprocess.PIPE,
                                 env=env)
        return process.stdout.decode('utf8')

    def _test_single_fixture(self, processor, parameters, data_in,
                             dp_out, data_out, env):
        """Test a single processor with the given fixture parameters"""
        output = self._run_processor(processor, parameters, data_in, env)
        self.test_fixture(output, dp_out, data_out)

    @staticmethod
    def test_fixture(processor_output, dp_out, data_out):
        '''Receives processor output and performs standard tests. Can be
        overridden in subclasses.'''
        (actual_dp, *actual_data) = processor_output.split('\n\n', 1)
        assert actual_dp == dp_out, \
            "unexpected value for output datapackage: {}".format(actual_dp)
        if len(actual_data) > 0:
            actual_data = actual_data[0]
            actual_data = actual_data.split('\n')
            expected_data = data_out.split('\n')
            assert len(actual_data) == len(expected_data), \
                "unexpected number of output lines: {}, actual_data = {}" \
                .format(len(actual_data), actual_data)
            for line_num, (actual, expected) in enumerate(zip(actual_data,
                                                              expected_data)):
                line_msg = "output line {}".format(line_num)
                if len(actual) == 0:
                    assert len(expected) == 0, \
                        "{}: did not get any data (but expected some)" \
                        .format(line_msg)
                else:
                    rj_actual = rejsonize(actual)
                    assert rj_actual == rejsonize(expected), \
                        "{}: unexpected data: {}".format(line_msg, rj_actual)


def rejsonize(s):
    return json.dumps(json.loads(s), sort_keys=True, ensure_ascii=True)


@mock.patch('datapackage_pipelines.wrapper.ingest')
@mock.patch('datapackage_pipelines.wrapper.spew')
def mock_processor_test(processor, ingest_tuple, mock_spew, mock_ingest):
    '''Helper function returns the `spew` for a given processor with a given
    `ingest` tuple.'''

    # Mock all calls to `ingest` to return `ingest_tuple`
    mock_ingest.return_value = ingest_tuple

    # Call processor
    file_path = processor
    module_name, _ = os.path.splitext(os.path.basename(file_path))
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Our processor called `spew`. Return the args it was called with.
    return mock_spew.call_args
