from dataflows import Flow, join, update_resource
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.utilities.flow_utils import spew_flow, load_lazy_json


def flow(parameters):
    source = parameters['source']
    target = parameters['target']
    return Flow(
        load_lazy_json(source['name']),
        join(
            source['name'],
            source['key'],
            target['name'],
            target['key'],
            parameters['fields'],
            parameters.get('full', True),
            source.get('delete', False)
        ),
        update_resource(
            target['name'],
            **{
                PROP_STREAMING: True
            }
        )
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
