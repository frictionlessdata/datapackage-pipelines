import os
import subprocess
import sys
import json

ROOT_PATH = os.path.join(os.path.dirname(__file__), '..', '..')
ENV = os.environ.copy()
ENV['PYTHONPATH'] = ROOT_PATH


def _test_single_fixture(processor, parameters, dp_in, data_in, dp_out, data_out):
    """Test a single processor with fixture"""

    process = subprocess.run([sys.executable, processor, '1', parameters, 'False', ''],
                             input=data_in, stdout=subprocess.PIPE, env=ENV)
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

prerecorded_responses = \
    os.walk(
        os.path.join(
            os.path.dirname(__file__),
            'fixtures'
        )
    )
for dirpath, _, filenames in prerecorded_responses:
    for filename in filenames:
        parts = \
            open(os.path.join(dirpath, filename), encoding='utf8').read().split('--\n')
        # print("PPP %s" % '\n~~~\n'.join(parts))
        (processor, params, dp_in, data_in, dp_out, data_out) = parts
        processor = processor.replace('.', '/')
        processor = os.path.join(ROOT_PATH, 'datapackage_pipelines', 'lib', processor.strip()+'.py')
        params = rejsonize(params)
        dp_out = rejsonize(dp_out)
        dp_in = rejsonize(dp_in)
        data_in = (dp_in + '\n\n' + data_in).encode('utf8')

        def inner(processor_, params_, dp_in_, data_in_, dp_out_, data_out_):
            def inner2():
                return _test_single_fixture(processor_, params_, dp_in_, data_in_, dp_out_, data_out_)
            return inner2
        globals()['test_stdlib_%s' % filename] = inner(processor, params, dp_in, data_in, dp_out, data_out)
