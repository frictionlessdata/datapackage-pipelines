import logging
import click

from .manager import execute_pipeline, finalize
from .manager.status import status
from .manager.specs import pipelines


@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx):
    if ctx.invoked_subcommand is None:
        click.echo('Available Pipelines:')
        for pipeline_id, _, _, dirty, errors in pipelines():
            click.echo('- {} {}{}'
                       .format(pipeline_id,
                               '(*)' if dirty else '',
                               '(E)' if len(errors) > 0 else ''))
            for short, long in errors:
                click.echo('\t{}: {}'.format(short, long))


@cli.command()
def serve():
    """Start the web server"""
    from .web import app
    app.run(host='0.0.0.0', debug=True, port=5000)


# pylint: disable=too-many-arguments
def execute_if_needed(argument, pipeline_id, pipeline_details,
                      dirty, errors,
                      pipeline_cwd, use_cache):
    if len(errors) != 0:
        return None, False
    if ((argument == pipeline_id) or
            (argument == 'all') or
            (argument == 'dirty' and dirty)):
        success, stats = \
            execute_pipeline(pipeline_id,
                             pipeline_details.get('pipeline', []),
                             pipeline_cwd,
                             use_cache=use_cache)
        stop = False
        if pipeline_id == argument:
            stop = True
        return (pipeline_id, success, stats), stop
    return None, False


@cli.command()
@click.argument('pipeline_id')
@click.option('--use-cache/--no-use-cache', default=True)
def run(pipeline_id, use_cache):
    """Run a pipeline by pipeline-id.
Use 'all' for running all pipelines,
or 'dirty' for running just the dirty ones."""
    try: # pylint: disable=too-many-nested-blocks
        results = []
        executed = set()
        modified = 1
        while modified > 0:
            modified = 0
            for pipeline in pipelines():
                _pipeline_id, pipeline_details, \
                    pipeline_cwd, dirty, errors = pipeline

                if _pipeline_id in executed:
                    continue

                ret, stop = \
                    execute_if_needed(pipeline_id, _pipeline_id,
                                      pipeline_details,
                                      dirty, errors,
                                      pipeline_cwd, use_cache)
                if ret is not None:
                    executed.add(_pipeline_id)
                    modified += 1
                    results.append(ret)

                if stop:
                    modified = 0
                    break

        logging.info('RESULTS:')
        for pipeline_id, success, stats in results:
            logging.info('%s: %s %s',
                         'SUCCESS' if success else 'FAILURE',
                         pipeline_id,
                         repr(stats) if stats is not None else '')

    except KeyboardInterrupt:
        pass
    finally:
        finalize()


@cli.command()
def init():
    """Reset the status of all pipelines"""
    status.initialize()
