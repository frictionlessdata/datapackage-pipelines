import click
import sys

from .utilities.stat_utils import user_facing_stats
from .utilities.execution_id import gen_execution_id

from .manager.logging_config import logging

from .specs import pipelines, register_all_pipelines, PipelineSpec #noqa
from .status import status
from .manager import execute_pipeline, finalize


@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx):
    if ctx.invoked_subcommand is None:
        click.echo('Available Pipelines:')
        for spec in pipelines():  # type: PipelineSpec
            ps = status.get(spec.pipeline_id)
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


def match_pipeline_id(arg, pipeline_id):
    if arg.endswith('*'):
        return pipeline_id.startswith(arg[:-1])
    else:
        return pipeline_id == arg


def execute_if_needed(argument, spec, use_cache):
    ps = status.get(spec.pipeline_id)
    if (match_pipeline_id(argument, spec.pipeline_id) or
            (argument == 'all') or
            (argument == 'dirty' and ps.dirty())):
        if len(spec.validation_errors) != 0:
            return (spec.pipeline_id, False, {}, ['init'] + list(map(str, spec.validation_errors))), \
                   spec.pipeline_id == argument
        eid = gen_execution_id()
        if ps.queue_execution(eid, 'manual'):
            success, stats, errors = \
                execute_pipeline(spec, eid,
                                 use_cache=use_cache)
            return (spec.pipeline_id, success, stats, errors), spec.pipeline_id == argument
        else:
            return (spec.pipeline_id, False, None, ['Already Running']), spec.pipeline_id == argument
    return None, False


@cli.command()
@click.argument('pipeline_id')
@click.option('--use-cache/--no-use-cache', default=True)
@click.option('--force', default=False, is_flag=True)
def run(pipeline_id, use_cache, force):
    """Run a pipeline by pipeline-id.
Use 'all' for running all pipelines,
or 'dirty' for running just the dirty ones."""
    exitcode = 0
    register_all_pipelines()
    try:
        results = []
        executed = set()
        modified = 1
        while modified > 0:
            modified = 0
            for spec in pipelines(ignore_missing_deps=force):

                if spec.pipeline_id in executed:
                    continue

                ret, stop = \
                    execute_if_needed(pipeline_id, spec, use_cache)

                if ret is not None:
                    executed.add(spec.pipeline_id)
                    modified += 1
                    results.append(ret)

                if stop:
                    modified = 0
                    break

        logging.info('RESULTS:')
        errd = False
        for pipeline_id, success, stats, errors in results:
            stats = user_facing_stats(stats)
            errd = errd or errors or not success
            logging.info('%s: %s %s%s',
                         'SUCCESS' if success else 'FAILURE',
                         pipeline_id,
                         repr(stats) if stats is not None else '',
                         ('\nERROR log from processor %s:\n+--------\n| ' % errors[0] +
                          '\n| '.join(errors[1:]) +
                          '\n+--------')
                         if errors else ''
                         )
        if errd:
            exitcode = 1

    except KeyboardInterrupt:
        pass
    finally:
        finalize()

    exit(exitcode)


@cli.command()
def init():
    """Reset the status of all pipelines"""
    status.initialize()


if __name__ == "__main__":
    sys.exit(cli())
