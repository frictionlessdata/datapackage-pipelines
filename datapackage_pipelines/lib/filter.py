from dataflows import Flow, filter_rows
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow


def flow(parameters):
    return Flow(
        filter_rows(
            equals=parameters.get('in', []),
            not_equals=parameters.get('out', []),
            resources=parameters.get('resources'),
        )
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
