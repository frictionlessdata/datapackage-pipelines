import sys
import json
import logging
import subprocess
from threading import Thread
from queue import Queue, Empty

import shutil
from celery import current_app
from .status import status

runner = '%-32s' % 'Main'
logging.basicConfig(level=logging.DEBUG,
                    format="%(levelname)-8s:"+runner+":%(message)s")


def enqueue_output(out, queue):
    for line in iter(out.readline, b''):
        line = line.rstrip()
        if line != '':
            print(line)
            queue.put(line)
    out.close()


def execute_pipeline(pipeline_id, pipeline_steps, trigger='manual'):

    status.running(pipeline_id, trigger, '')

    stdout = None
    stderr = Queue()

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
                                   stderr=subprocess.PIPE,
                                   universal_newlines=True)
        processes.append(process)
        stdout = process.stdout
        t = Thread(target=enqueue_output, args=(process.stderr, stderr))
        t.daemon = True # thread dies with the program
        t.start()

    if stdout is not None:
        shutil.copyfileobj(stdout, open(pipeline_id, 'w'))

    errors = []
    return_codes = []
    kill = False
    rounds = 0
    while len(processes) > 0:
        rounds += 1
        if rounds > 3600 and not kill:
            logging.error("TIMED OUT %s:%s", pipeline_id, process.args[1])
            kill = True

        ret_code = None
        for process in processes:
            if kill:
                logging.error("KILLING %s:%s", pipeline_id, process.args[1])
                process.kill()
                processes.remove(process)
                ret_code = 'KILLED'
                break
            else:
                # logging.info("WAITING FOR %s:%s", pipeline_id, process.args[1])
                try:
                    ret_code = process.wait(timeout=1)
                    kill = kill or ret_code != 0
                    processes.remove(process)
                    break
                except subprocess.TimeoutExpired:
                    pass

        while not stderr.empty():
            errors.append(stderr.get())
            if len(errors) > 1000:
                errors.pop(0)

        status.running(pipeline_id, trigger, '\n'.join(errors))

        if ret_code is not None:
            return_codes.append(ret_code)

    logging.info("DONE %s: %s", pipeline_id, return_codes)

    status.idle(pipeline_id,
                return_codes[-1] == 0,
                '\n'.join(errors))

    assert return_codes[-1] == 0


@current_app.task
def execute_pipeline_task(pipeline_id, pipeline_steps):
    execute_pipeline(pipeline_id, pipeline_steps)
