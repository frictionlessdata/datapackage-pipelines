import click

from .manager import execute_pipeline
from .manager.status import status
from .manager.specs import pipelines


@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx):
    if ctx.invoked_subcommand is None:
        click.echo('Available Pipelines:')
        for pipeline_id, _, _, dirty in pipelines():
            click.echo('- {} {}'.format(pipeline_id, '(*)' if dirty else ''))


@cli.command()
def serve():
    """Start the web server"""
    from .web import app
    app.run(host='0.0.0.0', debug=True, port=5000)


@cli.command()
@click.argument('pipeline_id')
@click.option('--use-cache/--no-use-cache', default=True)
def run(pipeline_id, use_cache):
    """Execute a single pipeline"""
    for _pipeline_id, pipeline_details, pipeline_cwd, dirty \
            in pipelines():
        if _pipeline_id == pipeline_id:
            execute_pipeline(pipeline_id,
                             pipeline_details['pipeline'],
                             pipeline_cwd,
                             use_cache=use_cache)
            break


@cli.command()
def init():
    """Reset the status of all pipelines"""
    status.initialize()
