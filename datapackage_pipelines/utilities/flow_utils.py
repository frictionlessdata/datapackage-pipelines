from dataflows import Flow, load, update_package
from dataflows.helpers.resource_matcher import ResourceMatcher

from datapackage_pipelines.wrapper import ProcessorContext
from datapackage_pipelines.utilities.extended_json import LazyJsonLine


def load_lazy_json(resources):
    resources = ResourceMatcher(resources)

    def func(rows):
        if resources.match(rows.res.name):
            for row in rows:
                if isinstance(row, LazyJsonLine):
                    yield row.inner
                else:
                    yield row
        else:
            yield from rows

    return func


def spew_flow(flow, ctx: ProcessorContext):
    flow = Flow(
        update_package(**ctx.datapackage),
        load((ctx.datapackage, ctx.resource_iterator)),
        flow,
    )
    datastream = flow.datastream()
    ctx.datapackage = datastream.dp.descriptor
    ctx.resource_iterator = datastream.res_iter
