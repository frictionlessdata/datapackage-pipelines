import sys
from importlib import import_module
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow


with ingest() as ctx:
    parameters, datapackage, resources = ctx
    stats = {}

    sys.path.append(parameters.pop('__path'))
    flow_module = import_module(parameters.pop('__flow'))
    flow = flow_module.flow(parameters, datapackage, resources, ctx.stats)

    spew_flow(flow, ctx)
