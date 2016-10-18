import gzip
import sys
import os
import json
import logging
import decimal
import datetime

from .input_processor import process_input


def processor():
    return "%-32s" % os.path.basename(sys.argv[0]).split('.')[0].title()

logging.basicConfig(level=logging.DEBUG,
                    format="%(levelname)-8s:"+processor()+":%(message)s")


class CommonJSONEncoder(json.JSONEncoder):
    """
    Common JSON Encoder
    json.dumps(myString, cls=CommonJSONEncoder)
    """

    def default(self, obj):     # pylint: disable=method-hidden

        if isinstance(obj, decimal.Decimal):
            return {'type{decimal}': str(obj)}
        elif isinstance(obj, datetime.date):
            return {'type{date}': str(obj)}


cache = ''


def ingest(debug=False):
    global cache # pylint: disable=global-statement
    params = None
    first = True
    validate = False
    if len(sys.argv) > 4:
        first = sys.argv[1] == '0'
        params = json.loads(sys.argv[2])
        validate = sys.argv[3] == 'True'
        cache = sys.argv[4]

    if first:
        return params, None, None

    datapackage, resource_iterator = process_input(sys.stdin, validate, debug)
    return params, datapackage, resource_iterator


def spew(dp, resources_iterator):
    files = [sys.stdout]

    cache_filename = ''
    if len(cache) > 0:
        if not os.path.exists('.cache'):
            os.mkdir('.cache')
        cache_filename = os.path.join('.cache', cache)
        files.append(gzip.open(cache_filename+'.ongoing', 'wt'))

    row_count = 0
    for f in files:
        f.write(json.dumps(dp, ensure_ascii=True)+'\n')
    for res in resources_iterator:
        for f in files:
            f.write('\n')
        for rec in res:
            line = json.dumps(rec, cls=CommonJSONEncoder, ensure_ascii=True)
            # logging.error('SPEWING: {}'.format(line))
            for f in files:
                f.write(line+'\n')
            # logging.error('WROTE')
            row_count += 1

    logging.info('Processed %d rows', row_count)

    for f in files:
        f.close()

    if len(cache) > 0:
        os.rename(cache_filename+'.ongoing', cache_filename)
