from dataflows import Flow, load, update_package
from dataflows.helpers.resource_matcher import ResourceMatcher

from datapackage_pipelines.wrapper import ProcessorContext
from datapackage_pipelines.utilities.extended_json import LazyJsonLine


def load_lazy_json(resources):

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        yield package.pkg
        for rows in package:
            if matcher.match(rows.res.name):
                yield (
                    row.inner
                    if isinstance(row, LazyJsonLine)
                    else row
                    for row in rows
                )
            else:
                yield rows

    return func


class MergeableStats():
    def __init__(self, ds_stats, ctx_stats):
        self.ds_stats = ds_stats
        self.ctx_stats = ctx_stats

    def __iter__(self):
        if self.ds_stats is not None:
            for x in self.ds_stats:
                yield from x.items()
        if self.ctx_stats is not None:
            yield from self.ctx_stats.items()


def spew_flow(flow, ctx: ProcessorContext):
    flow = Flow(
        update_package(**ctx.datapackage),
        load((ctx.datapackage, ctx.resource_iterator)),
        flow,
    )
    datastream = flow.datastream()
    ctx.datapackage = datastream.dp.descriptor
    ctx.resource_iterator = datastream.res_iter
    ctx.stats = MergeableStats(datastream.stats, ctx.stats)
