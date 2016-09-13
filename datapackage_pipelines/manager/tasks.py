import hashlib
import os
import sys
import json
import logging

import asyncio
from concurrent.futures import CancelledError

from celery import current_app
from .status import status
from .specs import resolve_executor

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


def create_process(args, cwd, wfd, rfd):
    pass_fds = {rfd, wfd}
    if None in pass_fds:
        pass_fds.remove(None)
    rfd = asyncio.subprocess.PIPE if rfd is None else rfd
    wfd = asyncio.subprocess.DEVNULL if wfd is None else wfd
    ret = asyncio.create_subprocess_exec(*args,
                                         stdin=rfd,
                                         stdout=wfd,
                                         stderr=asyncio.subprocess.PIPE,
                                         pass_fds=pass_fds,
                                         cwd=cwd)
    return ret

async def process_death_waiter(process):
    return_code = await process.wait()
    return process, return_code


def find_caches(pipeline_steps, pipeline_cwd):
    if not any(step.get('cache') for step in pipeline_steps):
        # If no step requires caching then bail
        return pipeline_steps

    cache_hash = ''
    for step in pipeline_steps:
        m = hashlib.md5()
        m.update(cache_hash.encode('ascii'))
        m.update(open(step['run'], 'rb').read())
        m.update(json.dumps(step, ensure_ascii=True, sort_keys=True)
                 .encode('ascii'))
        cache_hash = m.hexdigest()
        step['_cache_hash'] = cache_hash

    for i, step in reversed(list(enumerate(pipeline_steps))):
        cache_filename = os.path.join(pipeline_cwd,
                                      '.cache',
                                      step['_cache_hash'])
        if os.path.exists(cache_filename):
            logging.info('Found cache for step %d: %s', i, step['run'])
            pipeline_steps = pipeline_steps[i+1:]
            cache_loader = resolve_executor('cache_loader', '.')
            step = {
                'run': cache_loader,
                'parameters': {
                    'load-from': cache_filename
                }
            }
            pipeline_steps.insert(0, step)
            break

    return pipeline_steps


async def construct_process_pipeline(pipeline_steps, pipeline_cwd):
    error_collectors = []
    processes = []
    error_queue = asyncio.Queue()
    errors = []
    rfd = None

    error_aggregator = \
        asyncio.ensure_future(dequeue_errors(error_queue, errors))

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
            str(step.get('validate', False)),
            step.get('_cache_hash') if step.get('cache') else ''
        ]
        process = await create_process(args, pipeline_cwd, wfd, rfd)
        process.args = args[1]
        if wfd is not None:
            os.close(wfd)
        if rfd is not None:
            os.close(rfd)

        processes.append(process)
        rfd = new_rfd
        error_collectors.append(
            asyncio.ensure_future(enqueue_errors(process, error_queue))
        )
    return processes, error_collectors, error_queue, error_aggregator


async def async_execute_pipeline(pipeline_id,
                                 pipeline_steps,
                                 pipeline_cwd,
                                 trigger):

    status.running(pipeline_id, trigger, '')

    logging.info("RUNNING %s:", pipeline_id)

    pipeline_steps = find_caches(pipeline_steps, pipeline_cwd)

    processes, error_collectors, error_queue, error_aggregator = \
        await construct_process_pipeline(pipeline_steps, pipeline_cwd)

    def kill_all_processes():
        for to_kill in processes:
            try:
                to_kill.kill()
            except ProcessLookupError:
                pass

    pending = [asyncio.ensure_future(process_death_waiter(process))
               for process in processes]
    while len(pending) > 0:
        done = []
        try:
            done, pending = \
                await asyncio.wait(pending,
                                   return_when=asyncio.FIRST_COMPLETED)
        except CancelledError:
            kill_all_processes()

        for waiter in done:
            process, return_code = waiter.result()
            if return_code == 0:
                logging.info("DONE %s", process.args)
                processes = [p for p in processes if p.pid != process.pid]
            else:
                logging.error("FAILED %s: %s", process.args, return_code)
                kill_all_processes()

    await asyncio.gather(*error_collectors)
    await error_queue.put(None)
    await error_aggregator


def execute_pipeline(pipeline_id,
                     pipeline_steps,
                     pipeline_cwd,
                     trigger='manual'):

    loop = asyncio.get_event_loop()

    pipeline_task = \
        asyncio.ensure_future(async_execute_pipeline(pipeline_id,
                                                     pipeline_steps,
                                                     pipeline_cwd,
                                                     trigger))
    try:
        loop.run_until_complete(pipeline_task)
    except KeyboardInterrupt:
        logging.info("Caught keyboard interrupt. Cancelling tasks...")
        # pipeline_task.throw(e)
        pipeline_task.cancel()
        loop.run_forever()
        # pipeline_task.exception()
        logging.info("Caught keyboard interrupt. DONE!")
    finally:
        loop.close()

    loop.close()


@current_app.task
def execute_pipeline_task(pipeline_id, pipeline_steps, pipeline_cwd):
    execute_pipeline(pipeline_id, pipeline_steps, pipeline_cwd, 'schedule')
