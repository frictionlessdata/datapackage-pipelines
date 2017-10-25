import collections

from datapackage_pipelines.wrapper import ingest, spew

params, dp, res_iter = ingest()


def sink(res_iter_):
    for res in res_iter_:
        collections.deque(res, maxlen=0)
    yield from ()


spew({'name': 'boop', 'resources': []},
     sink(res_iter))
