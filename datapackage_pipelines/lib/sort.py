from dataflows import Flow, sort_rows
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow, load_lazy_json


def flow(parameters):
    return Flow(
        load_lazy_json(parameters.get('resources')),
        sort_rows(
            parameters['sort-by'],
            resources=parameters.get('resources'),
            reverse=parameters.get('reverse')
        )
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
