"""
This module contains code that can be used to run automated tests of datapackage pipelines processors

It is used internally to test the standard library processors but supports external use as well.
"""
import os
import subprocess
import sys
import json


class ProcessorFixtureTestsBase(object):

    def __init__(self, fixtures_path):
        self._fixtures_path = fixtures_path

    def get_tests(self):
        for dirpath, _, filenames in os.walk(self._fixtures_path):
            for filename in filenames:
                data_in, data_out, dp_out, params, processor = self._load_fixture(dirpath, filename)
                def inner(processor_, params_, data_in_, dp_out_, data_out_):
                    def inner2():
                        return test_single_fixture(processor_, params_, data_in_, dp_out_, data_out_, self._get_procesor_env())
                    return inner2
                yield filename, inner(processor, params, data_in, dp_out, data_out)

    def _get_procesor_env(self):
        """
        set the environment variables for the sub-process that runs the processor
        extending classes will most likely want to set the PYTHONPATH variable to correct value
        """
        return {}

    def _get_processor_file(self, processor):
        """
        should be implemented by extending classes to return the path to the processor python file
        :param processor: the value of the 1st part of the fixture (the name of the processor to test)
        :return: full path to the processor python file
        """
        raise NotImplementedError()

    def _load_fixture(self, dirpath, filename):
        parts = open(os.path.join(dirpath, filename), encoding='utf8').read().split('--\n')
        processor, params, dp_in, data_in, dp_out, data_out = parts
        processor_file = self._get_processor_file(processor)
        params = rejsonize(params)
        dp_out = rejsonize(dp_out)
        dp_in = rejsonize(dp_in)
        data_in = (dp_in + '\n\n' + data_in).encode('utf8')
        return data_in, data_out, dp_out, params, processor_file


def test_single_fixture(processor, parameters, data_in, dp_out, data_out, env):
    """Test a single processor with the given fixture parameters"""
    process = subprocess.run([sys.executable, processor, '1', parameters, 'False', ''],
                             input=data_in, stdout=subprocess.PIPE, env=env)
    output = process.stdout.decode('utf8')
    (actual_dp, *actual_data) = output.split('\n\n', 1)
    assert actual_dp == dp_out
    if len(actual_data) > 0:
        actual_data = actual_data[0]
        for actual, expected in zip(actual_data.split('\n'), data_out.split('\n')):
            if len(actual) == 0:
                assert len(expected) == 0
            else:
                assert rejsonize(actual) == rejsonize(expected)


def rejsonize(s):
    return json.dumps(json.loads(s), sort_keys=True, ensure_ascii=True)
