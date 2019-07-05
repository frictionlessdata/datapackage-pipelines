from dataflows import Flow, delete_fields
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow


def flow(parameters):
    resources = parameters.get('resources')
    regex = parameters.get('regex', True)
    return Flow(
        delete_fields(
            parameters.get('fields', []),
            resources=resources,
            regex=regex,
        )
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
