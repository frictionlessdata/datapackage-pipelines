import sys

from datapackage_pipelines.wrapper import ingest, spew

SINK_MAGIC = '>>> PROCESSED ROWS: '


def sink(res_iter_):
    count = 0
    for res in res_iter_:
        for row in res:
            count += 1
            if count % 100 == 0:
                sys.stderr.write('%s%d\n' % (SINK_MAGIC, count))
                sys.stderr.flush()
    sys.stderr.write('%s%d\n' % (SINK_MAGIC, count))
    sys.stderr.flush()
    yield from ()


if __name__ == '__main__':
    sys.stderr.write('%s%d\n' % (SINK_MAGIC, 0))
    sys.stderr.flush()
    params, dp, res_iter = ingest()
    spew({'name': 'boop', 'resources': []},
        sink(res_iter))
