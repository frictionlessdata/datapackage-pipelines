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
                data_in, data_out, dp_out, params, processor = self._load_fixture(dirpath, filename)
                yield filename, self._get_test_func(processor, params, data_in, dp_out, data_out, filename)

    def _get_test_func(self, processor, params, data_in, dp_out, data_out, filename):
        def inner():
            return self._test_single_fixture(processor, params, data_in, dp_out, data_out,
                                             self._get_procesor_env(filename))
        return inner

    def _get_procesor_env(self, filename):
        """
        Set the environment variables for the sub-process that runs the
        processor extending classes will most likely want to set the PYTHONPATH
        variable to correct value
        you can use the filename param to setup DB or other dependencies differently for each fixture
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
        # Hacky way to form cwd for resource path and ignore  other formating like %Y-%m-%d
        try:
            dp_out = dp_out % ({"base": os.getcwd()})
        except: #noqa
            pass
        dp_out = rejsonize(dp_out)
        dp_in = rejsonize(dp_in)
        data_in = reline(data_in)
        data_out = reline(data_out)
        data_in = (dp_in + '\n\n' + data_in).encode('utf8')
        return data_in, data_out, dp_out, params, processor_file

    def _run_processor(self, processor, parameters, data_in, env):
        '''Run the passed `processor` and return the output'''
        process = subprocess.run([sys.executable, processor, '1',
                                  parameters, 'False', ''],
                                 input=data_in,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 env=env)
        print('\nProcessor output:\n')
        for line in process.stderr.decode('utf8').split('\n'):
            print(f'OUT> {line}')
        if process.returncode != 0:
            raise Exception(f'processor execution failed with {process.returncode}')
        return process.stdout.decode('utf8')

    def _test_single_fixture(self, processor, parameters, data_in,
                             dp_out, data_out, env):
        """Test a single processor with the given fixture parameters.
           Handle dependencies info adding and removal
        """
        output = self._run_processor(processor, parameters, b'{}\n' + data_in, env)
        self.test_fixture(output[3:], dp_out, data_out)

    @staticmethod
    def test_fixture(processor_output, dp_out, data_out):
        '''Receives processor output and performs standard tests. Can be
        overridden in subclasses.'''
        (actual_dp, *actual_data) = processor_output.split('\n\n', 1)
        if actual_dp.strip() != dp_out.strip():
            print("unexpected value for output datapackage:\n{!r}\n{!r}".format(actual_dp, dp_out),
                  file=sys.stderr)
        actual_dp = json.loads(actual_dp)
        dp_out = json.loads(dp_out)
        for ares, eres in zip(actual_dp.get('resources', []), dp_out.get('resources', [])):
            assert ares.get('schema', {}).get('fields') == eres.get('schema', {}).get('fields')
            assert ares.get('schema', {}) == eres.get('schema', {})
            assert ares == eres, 'error comparing actual:\n%r\nto expected:\n%r\n...' % (ares, eres)
        assert actual_dp == dp_out

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
                    if rj_actual != rejsonize(expected):
                        print("{}: unexpected data: {} (expected {})".format(line_msg, rj_actual, expected),
                              file=sys.stderr)
                    assert json.loads(actual) == json.loads(expected), \
                        "a: %r, e: %r" % (json.loads(actual), json.loads(expected))


def rejsonize(s):
    return json.dumps(json.loads(s), sort_keys=True, ensure_ascii=True)


def reline(data):
    data = data.strip().split('\n')
    out = ''
    buf = ''
    for line in data:
        if not line.strip():
            out += '\n'
        buf += line
        try:
            buf = json.loads(buf)
        except Exception:
            continue
        out += json.dumps(buf, sort_keys=True, ensure_ascii=True) + '\n'
        buf = ''
    return out


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
