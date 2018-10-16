from dataflows import Flow, dump_to_zip
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow


def flow(parameters: dict):
    out_file = parameters.pop('out-file')
    return Flow(
        dump_to_zip(
            out_file,
            **parameters
        )
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
