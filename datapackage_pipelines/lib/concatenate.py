from dataflows import Flow, concatenate, update_resource
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.utilities.flow_utils import spew_flow


def flow(parameters):
    return Flow(
        concatenate(
            parameters.get('fields', {}),
            parameters.get('target', {}),
            parameters.get('sources')
        ),
        update_resource(
            parameters.get('target', {}).get('name', 'concat'),
            **{
                PROP_STREAMING: True
            }
        )
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
