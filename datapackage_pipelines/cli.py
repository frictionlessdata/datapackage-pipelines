import click

from .manager.logging_config import logging

from .specs import pipelines, register_all_pipelines
from .status import status
from .manager import execute_pipeline, finalize


@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx):
    if ctx.invoked_subcommand is None:
        click.echo('Available Pipelines:')
        for spec in pipelines():
            click.echo('- {} {}{}'
                       .format(spec.pipeline_id,
                               '(*)' if spec.dirty else '',
                               '(E)' if len(spec.errors) > 0 else ''))
            for error in spec.errors:
                click.echo('\t{}: {}'.format(error.short_msg,
                                             error.long_msg))


@cli.command()
def serve():
    """Start the web server"""
    from .web import app
    app.run(host='0.0.0.0', debug=True, port=5000)


def execute_if_needed(argument, spec, use_cache):
    if len(spec.errors) != 0:
        return None, False
    if ((argument == spec.pipeline_id) or
            (argument == 'all') or
            (argument == 'dirty' and spec.dirty)):
        success, stats = \
            execute_pipeline(spec,
                             use_cache=use_cache)
        stop = False
        if spec.pipeline_id == argument:
            stop = True
        return (spec.pipeline_id, success, stats), stop
    return None, False


@cli.command()
@click.argument('pipeline_id')
@click.option('--use-cache/--no-use-cache', default=True)
def run(pipeline_id, use_cache):
    """Run a pipeline by pipeline-id.
Use 'all' for running all pipelines,
or 'dirty' for running just the dirty ones."""
    register_all_pipelines()
    try:
        results = []
        executed = set()
        modified = 1
        while modified > 0:
            modified = 0
            for spec in pipelines():

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


if __name__ == "__main__":
    cli()
