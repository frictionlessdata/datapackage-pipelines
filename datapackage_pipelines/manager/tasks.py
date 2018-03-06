import asyncio
import gzip
import logging
import os
from concurrent.futures import CancelledError
from json.decoder import JSONDecodeError

from ..specs.specs import resolve_executor
from ..status import status_mgr
from ..utilities.stat_utils import STATS_DPP_KEY, STATS_OUT_DP_URL_KEY
from ..utilities.extended_json import json

from .runners import runner_config

SINK = os.path.join(os.path.dirname(__file__),
                    '..', 'lib', 'internal', 'sink.py')


async def enqueue_errors(step, process, queue, debug):
    out = process.stderr
    errors = []
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
            if len(errors) == 0:
                if line.startswith('ERROR') or line.startswith('Traceback'):
                    errors.append(step['run'])
            if len(errors) > 0:
                errors.append(line)
                if len(errors) > 1000:
                    errors.pop(1)
            line = "{}: {}".format(step['run'], line)
            if debug:
                logging.info(line)
            await queue.put(line)
    return errors


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
            try:
                canary = gzip.open(cache_filename, "rt")
                canary.seek(1)
                canary.close()
            except Exception:  #noqa
                continue
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


async def construct_process_pipeline(pipeline_steps, pipeline_cwd, errors, debug=False):
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

        if debug:
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
            asyncio.ensure_future(enqueue_errors(step, process, error_queue, debug))
        )

    error_collectors.append(
        asyncio.ensure_future(collect_stats(os.fdopen(rfd)))
    )

    def wait_for_finish(_error_collectors,
                        _error_queue,
                        _error_aggregator):
        async def _func(failed_index=None):
            *errors, count = await asyncio.gather(*_error_collectors)
            if failed_index is not None:
                errors = errors[failed_index]
            else:
                errors = None
            await _error_queue.put(None)
            await _error_aggregator
            return count, errors
        return _func

    return processes, \
        wait_for_finish(error_collectors,
                        error_queue,
                        error_aggregator)


async def async_execute_pipeline(pipeline_id,
                                 pipeline_steps,
                                 pipeline_cwd,
                                 trigger,
                                 execution_id,
                                 use_cache,
                                 dependencies,
                                 debug):

    if debug:
        logging.info("%s Async task starting", execution_id[:8])
    ps = status_mgr().get(pipeline_id)
    if not ps.start_execution(execution_id):
        logging.info("%s START EXECUTION FAILED %s, BAILING OUT", execution_id[:8], pipeline_id)
        return False, {}, []

    ps.update_execution(execution_id, [])

    if use_cache:
        if debug:
            logging.info("%s Searching for existing caches", execution_id[:8])
        pipeline_steps = find_caches(pipeline_steps, pipeline_cwd)
    execution_log = []

    if debug:
        logging.info("%s Building process chain:", execution_id[:8])
    processes, stop_error_collecting = \
        await construct_process_pipeline(pipeline_steps, pipeline_cwd, execution_log, debug)

    processes[0].stdin.write(json.dumps(dependencies).encode('utf8') + b'\n')
    processes[0].stdin.write(b'{"name": "_", "resources": []}\n')
    processes[0].stdin.close()

    def kill_all_processes():
        for to_kill in processes:
            try:
                to_kill.kill()
            except ProcessLookupError:
                pass

    success = True
    pending = [asyncio.ensure_future(process_death_waiter(process))
               for process in processes]
    index_for_pid = dict(
        (p.pid, i)
        for i, p in enumerate(processes)
    )
    failed_index = None
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
                if debug:
                    logging.info("%s DONE %s", execution_id[:8], process.args)
                processes = [p for p in processes if p.pid != process.pid]
            else:
                if return_code > 0 and failed_index is None:
                    failed_index = index_for_pid[process.pid]
                if debug:
                    logging.error("%s FAILED %s: %s", execution_id[:8], process.args, return_code)
                success = False
                kill_all_processes()

        if success and not ps.update_execution(execution_id, execution_log):
            logging.error("%s FAILED to update %s", execution_id[:8], pipeline_id)
            success = False
            kill_all_processes()

    stats, error_log = await stop_error_collecting(failed_index)
    if success is False:
        stats = None

    ps.update_execution(execution_id, execution_log, hooks=True)
    ps.finish_execution(execution_id, success, stats, error_log)

    logging.info("%s DONE %s %s %r", execution_id[:8], 'V' if success else 'X', pipeline_id, stats)

    return success, stats, error_log


def execute_pipeline(spec,
                     execution_id,
                     trigger='manual',
                     use_cache=True):

    debug = trigger == 'manual' or os.environ.get('DPP_DEBUG')
    logging.info("%s RUNNING %s", execution_id[:8], spec.pipeline_id)

    loop = asyncio.get_event_loop()

    if debug:
        logging.info("%s Collecting dependencies", execution_id[:8])
    dependencies = {}
    for dep in spec.pipeline_details.get('dependencies', []):
        if 'pipeline' in dep:
            dep_pipeline_id = dep['pipeline']
            pipeline_execution = status_mgr().get(dep_pipeline_id).get_last_successful_execution()
            if pipeline_execution is not None:
                result_dp = pipeline_execution.stats.get(STATS_DPP_KEY, {}).get(STATS_OUT_DP_URL_KEY)
                if result_dp is not None:
                    dependencies[dep_pipeline_id] = result_dp

    if debug:
        logging.info("%s Running async task", execution_id[:8])

    pipeline_task = \
        asyncio.ensure_future(async_execute_pipeline(spec.pipeline_id,
                                                     spec.pipeline_details.get('pipeline', []),
                                                     spec.path,
                                                     trigger,
                                                     execution_id,
                                                     use_cache,
                                                     dependencies,
                                                     debug))
    try:
        if debug:
            logging.info("%s Waiting for completion", execution_id[:8])
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
