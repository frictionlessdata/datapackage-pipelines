import os
import sys
import json
import logging

import asyncio
from celery import current_app
from .status import status

runner = '%-32s' % 'Main'
logging.basicConfig(level=logging.DEBUG,
                    format="%(levelname)-8s:"+runner+":%(message)s")


async def enqueue_errors(process, queue):
    out = process.stderr
    while True:
        line = await out.readline()
        if line == b'':
            break
        line = line.decode('utf8').rstrip()
        if len(line) != 0:
            print(line)
            await queue.put(line)


async def dequeue_errors(queue, out):
    while True:
        line = await queue.get()
        if line is None:
            break
        out.append(line)
        if len(out) > 1000:
            out.pop(0)


def create_process(args, wfd, rfd):
    pass_fds = {rfd, wfd}
    if None in pass_fds:
        pass_fds.remove(None)
    rfd = asyncio.subprocess.PIPE if rfd is None else rfd
    wfd = asyncio.subprocess.DEVNULL if wfd is None else wfd
    ret = asyncio.create_subprocess_exec(*args,
                                         stdin=rfd,
                                         stdout=wfd,
                                         stderr=asyncio.subprocess.PIPE,
                                         pass_fds=pass_fds)
    return ret

async def process_death_waiter(process):
    return_code = await process.wait()
    return process, return_code

async def async_execute_pipeline(pipeline_id, pipeline_steps, trigger='manual'):

    status.running(pipeline_id, trigger, '')

    rfd = None
    error_queue = asyncio.Queue()
    errors = []
    error_aggregator = asyncio.ensure_future(dequeue_errors(error_queue, errors))
    error_collectors = []

    processes = []
    logging.info("RUNNING %s:", pipeline_id)
    for i, step in enumerate(pipeline_steps):

        if i != len(pipeline_steps)-1:
            new_rfd, wfd = os.pipe()
        else:
            new_rfd, wfd = None, None

        logging.info("- %s", step['run'])
        args = [
            sys.executable,
            step['run'],
            str(i),
            json.dumps(step.get('parameters', {})),
            str(step.get('validate', False))
        ]
        process = await create_process(args, wfd, rfd)
        process.args = args[1]
        if wfd is not None:
            os.close(wfd)
        if rfd is not None:
            os.close(rfd)

        processes.append(process)
        rfd = new_rfd
        error_collectors.append(asyncio.ensure_future(enqueue_errors(process, error_queue)))

    pending = [asyncio.ensure_future(process_death_waiter(process)) for process in processes]
    while len(pending) > 0:
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        for waiter in done:
            process, return_code = waiter.result()
            if return_code == 0:
                logging.info("DONE %s", process.args)
                processes = [p for p in processes if p.pid != process.pid]
            else:
                logging.error("FAILED %s: %s", process.args, return_code)
                for to_kill in processes:
                    to_kill.kill()

    await asyncio.gather(*error_collectors)
    await error_queue.put(None)
    await error_aggregator


def execute_pipeline(pipeline_id, pipeline_steps):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_execute_pipeline(pipeline_id, pipeline_steps))
    loop.close()


@current_app.task
def execute_pipeline_task(pipeline_id, pipeline_steps):
    execute_pipeline(pipeline_id, pipeline_steps)
