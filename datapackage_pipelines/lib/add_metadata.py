import warnings

from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow

from datapackage_pipelines.lib.update_package import flow


if __name__ == '__main__':
    warnings.warn(
        'add_metadata will be removed in the future, use "update_package" instead',
        DeprecationWarning
    )
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
