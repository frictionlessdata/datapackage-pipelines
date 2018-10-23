from dataflows import Flow, duplicate
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow, load_lazy_json


def flow(parameters):
    return Flow(
        load_lazy_json(parameters.get('source')),
        duplicate(
            parameters.get('source'),
            parameters.get('target-name'),
            parameters.get('target-path'),
            parameters.get('batch_size', 1000)
        )
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
