from dataflows import Flow, load
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow
from datapackage_pipelines.utilities.resources import PROP_STREAMING, PROP_STREAMED_FROM


def flow(parameters):
    _from = parameters.pop('from')

    num_resources = 0

    def count_resources():
        def func(package):
            global num_resources
            num_resources = len(package.pkg.resources)
            yield package.pkg
            yield from package
        return func

    def mark_streaming(_from):
        def func(package):
            for i in range(num_resources, len(package.pkg.resources)):
                package.pkg.descriptor['resources'][i].setdefault(PROP_STREAMING, True)
                package.pkg.descriptor['resources'][i].setdefault(PROP_STREAMED_FROM,  _from)
            yield package.pkg
            yield from package
        return func

    return Flow(
        count_resources(),
        load(_from, **parameters),
        mark_streaming(_from),
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
