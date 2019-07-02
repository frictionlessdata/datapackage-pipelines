from dataflows import Flow, update_resource
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow


def flow(parameters):
    resources = parameters.get('resources', None)
    metadata = parameters.pop('metadata', {})
    return Flow(
        update_resource(resources, **metadata),
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
