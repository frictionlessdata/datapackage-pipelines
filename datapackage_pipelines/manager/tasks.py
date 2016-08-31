import sys
import json
import logging
import subprocess
from subprocess import TimeoutExpired

import shutil
from celery import current_app

runner = '%-32s' % 'Main'
logging.basicConfig(level=logging.DEBUG,
                    format="%(levelname)-8s:"+runner+":%(message)s")


def execute_pipeline(pipeline_id, pipeline_steps):
    stdout = None
    processes = []
    logging.info("RUNNING %s:", pipeline_id)
    for i, step in enumerate(pipeline_steps):
        logging.info("- %s", step['run'])
        args = [
            sys.executable,
            step['run'],
            str(i),
            json.dumps(step.get('parameters', {}))
        ]
        process = subprocess.Popen(args,
                                   stdin=stdout,
                                   stdout=subprocess.PIPE,
                                   stderr=sys.stdout)
        processes.append(process)
        stdout = process.stdout

    if stdout is not None:
        shutil.copyfileobj(stdout, open(pipeline_id, 'wb'))

    return_codes = []
    kill = False
    for process in processes:
        ret_code = 'KILLED'
        if kill:
            logging.error("KILLING %s:%s", pipeline_id, process.args[1])
            process.kill()
        else:
            logging.info("WAITING FOR %s:%s", pipeline_id, process.args[1])
            try:
                ret_code = process.wait(timeout=3600)
                kill = kill or ret_code != 0
            except TimeoutExpired:
                logging.error("TIMED OUT %s:%s", pipeline_id, process.args[1])
                kill = True
                process.kill()

        return_codes.append(ret_code)

    logging.info("DONE %s: %s", pipeline_id, return_codes)
    assert return_codes[-1] == 0


@current_app.task
def execute_pipeline_task(pipeline_id, pipeline_steps):
    execute_pipeline(pipeline_id, pipeline_steps)
