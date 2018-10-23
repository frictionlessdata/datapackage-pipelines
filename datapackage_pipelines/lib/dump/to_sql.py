import warnings

from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow

from datapackage_pipelines.lib.dump_to_sql import flow


if __name__ == '__main__':
    warnings.warn(
        'dump.to_sql will be removed in the future, use "dump_to_sql" instead',
        DeprecationWarning
    )
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
