import sys
import os
import json
import concurrent
import subprocess
import threading
from queue import Queue

from collections import namedtuple

from ..utilities.execution_id import gen_execution_id

from ..specs import pipelines, PipelineSpec #noqa
from ..status import status_mgr
from ..lib.internal.sink import SINK_MAGIC
from .tasks import execute_pipeline, finalize


ExecutionResult = namedtuple('ExecutionResult',
                             ['pipeline_id', 'success', 'stats', 'errors'])

ProgressReport = namedtuple('ProgressReport',
                            ['pipeline_id', 'row_count', 'success', 'errors', 'stats'])

MAGIC = 'INFO    :(sink): '+SINK_MAGIC


def remote_execute_pipeline(spec, root_dir, use_cache, verbose, progress_report_queue):
    args = ['dpp',
            'run',
            '--slave',
            '--use-cache' if use_cache else '--no-use-cache',
            spec.pipeline_id]
    popen = subprocess.Popen(args, encoding='utf8',
                             stderr=subprocess.PIPE,
                             stdout=subprocess.DEVNULL,
                             env=os.environ.copy(),
                             cwd=root_dir)
    progress = 0
    lines = []
    for line in popen.stderr:
        if len(line) == 0:
            continue
        if line.startswith(MAGIC):
            if progress_report_queue is not None:
                progress = int(line[len(MAGIC):].strip())
                progress_report_queue.put(ProgressReport(spec.pipeline_id, progress, None, None, None))
            continue
        while len(lines) > 0:
            log = lines.pop(0)
            if verbose:
                sys.stderr.write('[%s:%s] >>> %s' %
                                 (spec.pipeline_id, threading.current_thread().name, log))
        lines.append(line)
    if len(lines) > 0:
        results = lines.pop(0)
    else:
        if progress_report_queue is not None:
            progress_report_queue.put(ProgressReport(spec.pipeline_id,
                                                     progress,
                                                     False,
                                                     ['Empty'],
                                                     None
                                                     ))
        return (spec.pipeline_id, False, {}, ['Empty'])
    try:
        results = json.loads(results)
        if progress_report_queue is not None:
            progress_report_queue.put(ProgressReport(spec.pipeline_id,
                                                     progress,
                                                     results[0]['success'],
                                                     results[0]['errors'],
                                                     results[0]['stats']
                                                     ))
    except json.decoder.JSONDecodeError:
        if verbose:
            sys.stderr.write('[%s:%s] >>> %s' % (spec.pipeline_id, threading.current_thread().name, results))
        if progress_report_queue is not None:
            progress_report_queue.put(ProgressReport(spec.pipeline_id,
                                                     progress,
                                                     False,
                                                     ['Crashed', results],
                                                     None
                                                     ))
        return (spec.pipeline_id, False, {}, [results])

    results = results[0]
    return (spec.pipeline_id,
            results['success'],
            results['stats'],
            results['errors'])


def progress_report_handler(callback, queue):
    while True:
        report = queue.get()
        if report is not None:
            callback(report)
        else:
            return


def match_pipeline_id(arg, pipeline_id):
    if arg == 'all':
        return True
    elif arg.endswith('%'):
        return pipeline_id.startswith(arg[:-1])
    else:
        return pipeline_id == arg


def specs_to_execute(argument, root_dir, status_manager, ignore_missing_deps, dirty, results):

    pending = set()
    executed = set()
    completed = set()

    for spec in pipelines(ignore_missing_deps=ignore_missing_deps,
                          root_dir=root_dir, status_manager=status_manager):
        if match_pipeline_id(argument, spec.pipeline_id):

            # If only dirty was requested
            if dirty:
                ps = status_manager.get(spec.pipeline_id)
                if not ps.dirty():
                    continue

            pending.add(spec.pipeline_id)

    while len(pending) > 0:
        to_yield = None
        for spec in pipelines(ignore_missing_deps=ignore_missing_deps,
                              root_dir=root_dir, status_manager=status_manager):
            pipeline_id = spec.pipeline_id
            if pipeline_id not in pending:
                continue

            unresolved = set(spec.dependencies) - completed
            if len(unresolved) == 0:
                to_yield = spec
                break

            unresolved = unresolved - executed - pending
            if len(unresolved) > 0:
                # No point in waiting for dependencies that will never be resolved
                to_yield = spec
                break

        if to_yield is not None:
            executed.add(to_yield.pipeline_id)
            pending.remove(to_yield.pipeline_id)
        completed_pipeline_id = yield(to_yield)
        if completed_pipeline_id is not None:
            completed.add(completed_pipeline_id)

    yield None


def run_pipelines(pipeline_id_pattern,
                  root_dir,
                  use_cache=True,
                  dirty=False,
                  force=False,
                  concurrency=1,
                  verbose_logs=True,
                  progress_cb=None,
                  slave=False):
    """Run a pipeline by pipeline-id.
       pipeline-id supports the '%' wildcard for any-suffix matching.
       Use 'all' or '%' for running all pipelines"""
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency,
                                               thread_name_prefix='T') as executor:
        try:
            results = []
            pending_futures = set()
            done_futures = set()
            finished_futures = []
            progress_thread = None
            progress_queue = None
            status_manager = status_mgr(root_dir)

            if progress_cb is not None:
                progress_queue = Queue()
                progress_thread = threading.Thread(target=progress_report_handler, args=(progress_cb, progress_queue))
                progress_thread.start()

            all_specs = specs_to_execute(pipeline_id_pattern, root_dir, status_manager, force, dirty, results)

            while True:

                done = None
                if len(done_futures) > 0:
                    done = done_futures.pop()
                    finished_futures.append(done)
                    done = done.result()[0]

                try:
                    spec = all_specs.send(done)
                except StopIteration:
                    spec = None

                if spec is None:
                    # Wait for all runners to idle...
                    if len(done_futures) == 0:
                        if len(pending_futures) > 0:
                            done_futures, pending_futures = \
                                concurrent.futures.wait(pending_futures,
                                                        return_when=concurrent.futures.FIRST_COMPLETED)
                            continue
                        else:
                            break
                    else:
                        continue

                if len(spec.validation_errors) > 0:
                    results.append(
                        ExecutionResult(spec.pipeline_id,
                                        False,
                                        {},
                                        ['init'] + list(map(str, spec.validation_errors)))
                    )
                    continue

                if slave:
                    ps = status_manager.get(spec.pipeline_id)
                    ps.init(spec.pipeline_details,
                            spec.source_details,
                            spec.validation_errors,
                            spec.cache_hash)
                    eid = gen_execution_id()
                    if ps.queue_execution(eid, 'manual'):
                        success, stats, errors = \
                            execute_pipeline(spec, eid,
                                             use_cache=use_cache)
                        results.append(ExecutionResult(
                            spec.pipeline_id,
                            success,
                            stats,
                            errors
                        ))
                    else:
                        results.append(
                            ExecutionResult(spec.pipeline_id,
                                            False,
                                            None,
                                            ['Already Running'])
                        )

                else:
                    f = executor.submit(remote_execute_pipeline,
                                        spec,
                                        root_dir,
                                        use_cache,
                                        verbose_logs,
                                        progress_queue)
                    pending_futures.add(f)

            for f in finished_futures:
                ret = f.result()
                results.append(ExecutionResult(*ret))

        except KeyboardInterrupt:
            pass
        finally:
            if slave:
                finalize()

            if progress_thread is not None:
                progress_queue.put(None)
                progress_thread.join()

    return results
