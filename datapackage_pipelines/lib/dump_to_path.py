import os

from dataflows import Flow, dump_to_path
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow

from datapackage_pipelines.utilities.stat_utils import STATS_DPP_KEY, STATS_OUT_DP_URL_KEY


def flow(parameters: dict, stats: dict):
    out_path = parameters.pop('out-path', '.')
    stats.setdefault(STATS_DPP_KEY, {})[STATS_OUT_DP_URL_KEY] = os.path.join(out_path, 'datapackage.json')
    return Flow(
        dump_to_path(
            out_path,
            **parameters
        )
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters, ctx.stats), ctx)
