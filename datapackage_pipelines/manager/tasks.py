import asyncio
import logging
import os
from concurrent.futures import CancelledError
from json.decoder import JSONDecodeError

from datapackage_pipelines.specs.specs import resolve_executor
from datapackage_pipelines.status import status
from ..utilities.extended_json import json

from .runners import runner_config

SINK = os.path.join(os.path.dirname(__file__),
                    '..', 'lib', 'internal', 'sink.py')


async def enqueue_errors(step, process, queue):
    out = process.stderr
    while True:
        try:
            line = await out.readline()
        except ValueError:
            logging.error('Received a too long log line (>64KB), discarded')
            continue
        if line == b'':
            break
        line = line.decode('utf8').rstrip()
        if len(line) != 0:
            line = "{}: {}".format(step['run'], line)
            logging.info(line)
            await queue.put(line)


async def dequeue_errors(queue, out):
    while True:
        line = await queue.get()
        if line is None:
            break
        out.append(line)
        if len(out) > 1000:
            out.pop(0)


async def collect_stats(infile):
    reader = asyncio.StreamReader()
    reader_protocol = asyncio.StreamReaderProtocol(reader)
    transport, _ = await asyncio.get_event_loop() \
        .connect_read_pipe(lambda: reader_protocol, infile)
    count = 0
    dp = None
    stats = None
    while True:
        try:
            line = await reader.readline()
        except ValueError:
            logging.exception('Too large stats object!')
            break
        if line == b'':
            break
        stats = line
        if dp is None:
            try:
                dp = json.loads(line.decode('ascii'))
            except JSONDecodeError:
                break
        count += 1

    transport.close()

    if dp is None or count == 0:
        return {}

    try:
        stats = json.loads(stats.decode('ascii'))
    except JSONDecodeError:
        stats = {}

    return stats


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

    for i, step in reversed(list(enumerate(pipeline_steps))):
        cache_filename = os.path.join(pipeline_cwd,
                                      '.cache',
                                      step['_cache_hash'])
        if os.path.exists(cache_filename):
            logging.info('Found cache for step %d: %s', i, step['run'])
            pipeline_steps = pipeline_steps[i+1:]
            step = {
                'run': 'cache_loader',
                'parameters': {
                    'load-from': os.path.join('.cache', step['_cache_hash'])
                }
            }
            step['executor'] = resolve_executor(step, '.', [])
            pipeline_steps.insert(0, step)
            break

    return pipeline_steps


async def construct_process_pipeline(pipeline_steps, pipeline_cwd, errors):
    error_collectors = []
    processes = []
    error_queue = asyncio.Queue()
    rfd = None

    error_aggregator = \
        asyncio.ensure_future(dequeue_errors(error_queue, errors))

    pipeline_steps.append({
        'run': '(sink)',
        'executor': SINK,
        '_cache_hash': pipeline_steps[-1]['_cache_hash']
    })

    for i, step in enumerate(pipeline_steps):

        new_rfd, wfd = os.pipe()

        logging.info("- %s", step['run'])
        runner = runner_config.get_runner(step.get('runner'))
        args = runner.get_execution_args(step, pipeline_cwd, i)
        process = await create_process(args, pipeline_cwd, wfd, rfd)
        process.args = args[1]
        if wfd is not None:
            os.close(wfd)
        if rfd is not None:
            os.close(rfd)

        processes.append(process)
        rfd = new_rfd
        error_collectors.append(
            asyncio.ensure_future(enqueue_errors(step, process, error_queue))
        )

    error_collectors.append(
        asyncio.ensure_future(collect_stats(os.fdopen(rfd)))
    )

    def wait_for_finish(_error_collectors,
                        _error_queue,
                        _error_aggregator):
        async def _func():
            *_, count = await asyncio.gather(*_error_collectors)
            await _error_queue.put(None)
            await _error_aggregator
            return count
        return _func

    return processes, \
        wait_for_finish(error_collectors,
                        error_queue,
                        error_aggregator)


async def async_execute_pipeline(pipeline_id,
                                 pipeline_steps,
                                 pipeline_cwd,
                                 trigger,
                                 use_cache):

    if status.is_running(pipeline_id):
        logging.info("ALREADY RUNNING %s, BAILING OUT", pipeline_id)
        return False, {}

    status.running(pipeline_id, trigger, '')

    logging.info("RUNNING %s", pipeline_id)

    if use_cache:
        pipeline_steps = find_caches(pipeline_steps, pipeline_cwd)
    errors = []

    processes, stop_error_collecting = \
        await construct_process_pipeline(pipeline_steps, pipeline_cwd, errors)

    def kill_all_processes():
        for to_kill in processes:
            try:
                to_kill.kill()
            except ProcessLookupError:
                pass

    success = True
    pending = [asyncio.ensure_future(process_death_waiter(process))
               for process in processes]
    while len(pending) > 0:
        done = []
        try:
            done, pending = \
                await asyncio.wait(pending,
                                   return_when=asyncio.FIRST_COMPLETED,
                                   timeout=10)
        except CancelledError:
            success = False
            kill_all_processes()

        for waiter in done:
            process, return_code = waiter.result()
            if return_code == 0:
                logging.info("DONE %s", process.args)
                processes = [p for p in processes if p.pid != process.pid]
            else:
                logging.error("FAILED %s: %s", process.args, return_code)
                success = False
                kill_all_processes()

        status.running(pipeline_id,
                       trigger,
                       '\n'.join(errors))

    stats = await stop_error_collecting()
    if success is False:
        stats = None

    cache_hash = ''
    if len(pipeline_steps) > 0:
        cache_hash = pipeline_steps[-1]['_cache_hash']

    status.idle(pipeline_id,
                success,
                '\n'.join(errors),
                cache_hash,
                stats)

    return success, stats


def execute_pipeline(spec,
                     trigger='manual',
                     use_cache=True):

    loop = asyncio.get_event_loop()

    pipeline_task = \
        asyncio.ensure_future(async_execute_pipeline(spec.pipeline_id,
                                                     spec.pipeline_details.get('pipeline', []),
                                                     spec.path,
                                                     trigger,
                                                     use_cache))
    try:
        return loop.run_until_complete(pipeline_task)
    except KeyboardInterrupt:
        logging.info("Caught keyboard interrupt. Cancelling tasks...")
        pipeline_task.cancel()
        loop.run_forever()
        logging.info("Caught keyboard interrupt. DONE!")
        raise KeyboardInterrupt()


def finalize():
    loop = asyncio.get_event_loop()
    loop.close()
