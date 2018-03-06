import gzip
import sys
import os
import logging

from tableschema.exceptions import CastError

from ..utilities.extended_json import json

from .input_processor import process_input

from ..utilities.resources import streaming


logging.basicConfig(level=logging.DEBUG,
                    format="%(levelname)-8s:%(message)s")


cache = ''
first = True

dependency_datapackage_urls = {}


def get_dependency_datapackage_url(pipeline_id):
    return dependency_datapackage_urls.get(pipeline_id)


def ingest(debug=False):
    global cache
    global first
    params = None
    validate = False
    if len(sys.argv) > 4:
        first = sys.argv[1] == '0'
        params = json.loads(sys.argv[2])
        validate = sys.argv[3] == 'True'
        cache = sys.argv[4]

    datapackage, resource_iterator, dependency_dp = process_input(sys.stdin, validate, debug)
    dependency_datapackage_urls.update(dependency_dp)

    return params, datapackage, resource_iterator


def spew(dp, resources_iterator, stats=None, finalizer=None):
    files = [sys.stdout]

    cache_filename = ''
    if len(cache) > 0:
        if not os.path.exists('.cache'):
            os.mkdir('.cache')
        cache_filename = os.path.join('.cache', cache)
        files.append(gzip.open(cache_filename+'.ongoing', 'wt'))

    expected_resources = \
        len(list(filter(streaming, dp.get('resources', []))))
    row_count = 0
    try:
        for f in files:
            f.write(json.dumps(dp, sort_keys=True, ensure_ascii=True)+'\n')
            f.flush()
        num_resources = 0
        for res in resources_iterator:
            num_resources += 1
            for f in files:
                f.write('\n')
            try:
                for rec in res:
                    try:
                        line = json.dumpl(rec,
                                        sort_keys=True,
                                        ensure_ascii=True)
                    except TypeError as e:
                        logging.error('Failed to encode row to JSON: %s\nOffending row: %r', e, rec)
                        raise
                    # logging.error('SPEWING: {}'.format(line))
                    for f in files:
                        f.write(line+'\n')
                    # logging.error('WROTE')
                    row_count += 1
            except CastError as e:
                for err in e.errors:
                    logging.error('Failed to cast row: %s', err)
                raise
        if num_resources != expected_resources:
            logging.error('Expected to see %d resource(s) but spewed %d',
                          expected_resources, num_resources)
            assert num_resources == expected_resources

        aggregated_stats = {}
        if not first:
            stats_line = sys.stdin.readline().strip()
            if len(stats_line) > 0:
                try:
                    aggregated_stats = json.loads(stats_line)
                    assert aggregated_stats is None or \
                        isinstance(aggregated_stats, dict)
                except json.JSONDecodeError:
                    logging.error('Failed to parse stats: %r', stats_line)
        if stats is not None:
            aggregated_stats.update(stats)
        stats_json = json.dumps(aggregated_stats,
                                sort_keys=True,
                                ensure_ascii=True)
        for f in files:
            f.write('\n'+stats_json)

    except BrokenPipeError:
        logging.error('Output pipe disappeared!')
        sys.stderr.flush()
        sys.exit(1)

    sys.stdout.flush()
    if row_count > 0:
        logging.info('Processed %d rows', row_count)

    if finalizer is not None:
        finalizer()

    for f in files:
        f.write('\n')  # Signal to other processors that we're done
        if f == sys.stdout:
            # Can't close sys.stdout, otherwise any subsequent
            # call to print() will throw an exception
            f.flush()
        else:
            f.close()

    if len(cache) > 0:
        os.rename(cache_filename+'.ongoing', cache_filename)


def generic_process_resource(rows,
                             spec,
                             resource_index,
                             parameters,
                             stats,
                             process_row):
    for row_index, row in enumerate(rows):
        row = process_row(row, row_index,
                          spec, resource_index,
                          parameters, stats)
        if row is not None:
            yield row


def generic_process_resources(resource_iterator,
                              parameters,
                              stats,
                              process_row):
    for resource_index, resource in enumerate(resource_iterator):
        rows = resource
        spec = resource.spec
        yield generic_process_resource(rows,
                                       spec,
                                       resource_index,
                                       parameters,
                                       stats,
                                       process_row)


def process(modify_datapackage=None,
            process_row=None, debug=False):
    stats = {}
    parameters, datapackage, resource_iterator = ingest(debug=debug)
    if modify_datapackage is not None:
        datapackage = modify_datapackage(datapackage, parameters, stats)

    if process_row is not None:
        new_iter = generic_process_resources(resource_iterator,
                                             parameters,
                                             stats,
                                             process_row)
        spew(datapackage, new_iter, stats)
    else:
        spew(datapackage, resource_iterator, stats)
