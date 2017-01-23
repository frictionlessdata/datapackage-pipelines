import gzip
import sys
import os
import logging
from ..utilities.extended_json import json

from .input_processor import process_input


def processor():
    return "%-32s" % os.path.basename(sys.argv[0]).split('.')[0].title()

logging.basicConfig(level=logging.DEBUG,
                    format="%(levelname)-8s:"+processor()+":%(message)s")


cache = ''
first = True


def ingest(debug=False):
    global cache  # pylint: disable=global-statement
    global first  # pylint: disable=global-statement
    params = None
    validate = False
    if len(sys.argv) > 4:
        first = sys.argv[1] == '0'
        params = json.loads(sys.argv[2])
        validate = sys.argv[3] == 'True'
        cache = sys.argv[4]

    if first:
        return params, {'name': '_', 'resources': []}, []

    datapackage, resource_iterator = process_input(sys.stdin, validate, debug)
    return params, datapackage, resource_iterator


# pylint: disable=too-many-branches
def spew(dp, resources_iterator, stats=None):
    files = [sys.stdout]

    cache_filename = ''
    if len(cache) > 0:
        if not os.path.exists('.cache'):
            os.mkdir('.cache')
        cache_filename = os.path.join('.cache', cache)
        files.append(gzip.open(cache_filename+'.ongoing', 'wt'))

    expected_resources = len(list(filter(lambda x:x.get('path') is not None, dp.get('resources', []))))
    row_count = 0
    try:
        for f in files:
            f.write(json.dumps(dp, sort_keys=True, ensure_ascii=True)+'\n')
        num_resources = 0
        for res in resources_iterator:
            num_resources += 1
            for f in files:
                f.write('\n')
            for rec in res:
                line = json.dumps(rec,
                                  sort_keys=True,
                                  ensure_ascii=True)
                # logging.error('SPEWING: {}'.format(line))
                for f in files:
                    f.write(line+'\n')
                # logging.error('WROTE')
                row_count += 1
        if num_resources != expected_resources:
            logging.error('Expected to see %d resource(s) but spewed %d',
                          expected_resources, num_resources)
            assert(num_resources == expected_resources)

        aggregated_stats = {}
        if not first:
            stats_line = sys.stdin.readline().strip()
            if len(stats_line) > 0:
                try:
                    aggregated_stats = json.loads(stats_line)
                    assert(aggregated_stats is None or type(aggregated_stats) is dict)
                except json.JSONDecodeError:
                    logging.error('Failed to parse stats: %r', stats_line)
        if stats is not None:
            aggregated_stats.update(stats)
        stats_json = json.dumps(aggregated_stats,
                                sort_keys=True,
                                ensure_ascii=True)
        for f in files:
            f.write('\n'+stats_json+'\n')

    except BrokenPipeError:
        logging.error('Output pipe disappeared!')
        sys.stderr.close()
        sys.exit(1)

    if row_count > 0:
        logging.info('Processed %d rows', row_count)

    for f in files:
        f.close()

    if len(cache) > 0:
        os.rename(cache_filename+'.ongoing', cache_filename)
