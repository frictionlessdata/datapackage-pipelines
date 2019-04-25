from dataflows import Flow, add_computed_field
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow


def flow(parameters):
    return Flow(
        add_computed_field(
            parameters.get('fields', []),
            resources=parameters.get('resources')
        ),
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
