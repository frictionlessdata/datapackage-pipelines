from dataflows import Flow, load, add_metadata

from datapackage_pipelines.wrapper import ProcessorContext


def spew_flow(flow, ctx: ProcessorContext):
    flow = Flow(
        add_metadata(**ctx.datapackage),
        load((ctx.datapackage, ctx.resource_iterator)),
        flow,
    )
    datastream = flow.datastream()
    ctx.datapackage = datastream.dp.descriptor
    ctx.resource_iterator = datastream.res_iter
