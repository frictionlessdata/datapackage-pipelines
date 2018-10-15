from dataflows import Flow, dump_to_path
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow


def flow(parameters: dict):
    out_path = parameters.pop('out-path', '.')
    return Flow(
        dump_to_path(
            out_path,
            **parameters
        )
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
