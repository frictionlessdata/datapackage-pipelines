from datapackage_pipelines.wrapper import ingest
import subprocess
import logging
import os


with ingest() as ctx:
    parameters, datapackage, resources = ctx
    assert len(datapackage['resources']) == 0, 'exec processor does not support input data'
    cmd = parameters.pop('__exec')
    os.environ['__EXEC_PROCESSOR_PATH'] = parameters.pop('__path')
    with subprocess.Popen(cmd, shell=isinstance(cmd, str), stdout=subprocess.PIPE) as p:
        for line in p.stdout:
            logging.info(line.decode())
        p.wait()
        assert p.returncode == 0, f'exec failed, returncode = {p.returncode}'
