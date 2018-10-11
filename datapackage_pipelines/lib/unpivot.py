from dataflows import Flow, unpivot
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow


def flow(parameters):
    return Flow(
        unpivot(
            parameters.get('unpivot'),
            parameters.get('extraKeyFields'),
            parameters.get('extraValueField'),
            resources=parameters.get('resources')
        )
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
