from dataflows import Flow, load, update_package

from datapackage_pipelines.wrapper import ProcessorContext


def spew_flow(flow, ctx: ProcessorContext):
    flow = Flow(
        update_package(**ctx.datapackage),
        load((ctx.datapackage, ctx.resource_iterator)),
        flow,
    )
    datastream = flow.datastream()
    ctx.datapackage = datastream.dp.descriptor
    ctx.resource_iterator = datastream.res_iter
