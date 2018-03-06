import sys
import json

import click

from .utilities.stat_utils import user_facing_stats

from .manager.logging_config import logging

from .specs import pipelines, PipelineSpec #noqa
from .status import status_mgr
from .manager import run_pipelines


@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx):
    if ctx.invoked_subcommand is None:
        click.echo('Available Pipelines:')
        for spec in pipelines():  # type: PipelineSpec
            ps = status_mgr().get(spec.pipeline_id)
            click.echo('- {} {}{}'
                       .format(spec.pipeline_id,
                               '(*)' if ps.dirty() else '',
                               '(E)' if len(spec.validation_errors) > 0 else ''))
            for error in spec.validation_errors:
                click.echo('\t{}: {}'.format(error.short_msg,
                                             error.long_msg))


@cli.command()
def serve():
    """Start the web server"""
    from .web import app
    app.run(host='0.0.0.0', debug=True, port=5000)


@cli.command()
@click.argument('pipeline_id')
@click.option('--verbose', default=False, is_flag=True)
@click.option('--use-cache/--no-use-cache', default=True,
              help='Cache (or don\'t) intermediate results (if requested in the pipeline)')
@click.option('--dirty', default=False, is_flag=True,
              help='Only run dirty pipelines')
@click.option('--force', default=False, is_flag=True)
@click.option('--concurrency', default=1)
@click.option('--slave', default=False, is_flag=True)
def run(pipeline_id, verbose, use_cache, dirty, force, concurrency, slave):
    """Run a pipeline by pipeline-id.
       pipeline-id supports the '%' wildcard for any-suffix matching.
       Use 'all' or '%' for running all pipelines"""
    exitcode = 0

    running = []
    progress = {}

    def progress_cb(report):
        pid, count, success, *_, stats = report

        print('\x1b[%sA' % (1+len(running)))
        if pid not in progress:
            running.append(pid)
        progress[pid] = count, success

        for pid in running:
            count, success = progress[pid]
            if success is None:
                if count == 0:
                    print('\x1b[2K%s: \x1b[31m%s\x1b[0m' % (pid, 'WAITING FOR OUTPUT'))
                else:
                    print('\x1b[2K%s: \x1b[33mRUNNING, processed %s rows\x1b[0m' % (pid, count))
            else:
                if success:
                    print('\x1b[2K%s: \x1b[32mSUCCESS, processed %s rows\x1b[0m' % (pid, count))
                else:
                    print('\x1b[2K%s: \x1b[31mFAILURE, processed %s rows\x1b[0m' % (pid, count))

    results = run_pipelines(pipeline_id, '.', use_cache,
                            dirty, force, concurrency,
                            verbose, progress_cb, slave)
    if not slave:
        logging.info('RESULTS:')
        errd = False
        for result in results:
            stats = user_facing_stats(result.stats)
            errd = errd or result.errors or not result.success
            logging.info('%s: %s %s%s',
                         'SUCCESS' if result.success else 'FAILURE',
                         result.pipeline_id,
                         repr(stats) if stats is not None else '',
                         (
                            '\nERROR log from processor %s:\n+--------\n| ' % result.errors[0] +
                            '\n| '.join(result.errors[1:]) +
                            '\n+--------'
                         ) if result.errors else '')
    else:
        result_obj = []
        errd = False
        for result in results:
            errd = errd or result.errors or not result.success
            stats = user_facing_stats(result.stats)
            result_obj.append(dict(
                success=result.success,
                pipeline_id=result.pipeline_id,
                stats=result.stats,
                errors=result.errors
            ))
            json.dump(result_obj, sys.stderr)

    if errd:
        exitcode = 1

    exit(exitcode)


@cli.command()
def init():
    """Reset the status of all pipelines"""
    status_mgr().initialize()


if __name__ == "__main__":
    sys.exit(cli())
    # For Profiling:
    # import cProfile
    # sys.exit(cProfile.run('cli()', sort='cumulative'))
