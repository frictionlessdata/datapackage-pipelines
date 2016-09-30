import copy
import gzip
import sys
import os
import json
import logging
import decimal
import datetime

import datapackage
from jsontableschema.exceptions import InvalidCastError
from jsontableschema.model import SchemaModel


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


class CommonJSONDecoder(json.JSONDecoder):
    """
    Common JSON Encoder
    json.loads(myString, cls=CommonJSONEncoder)
    """

    @classmethod
    def object_hook(cls, obj):  # pylint: disable=method-hidden
        if 'type{decimal}' in obj:
            try:
                return decimal.Decimal(obj['type{decimal}'])
            except decimal.InvalidOperation:
                pass
        if 'type{date}' in obj:
            try:
                return datetime.datetime \
                    .strptime(obj["type{date}"], '%Y-%m-%d') \
                    .date()
            except ValueError:
                pass

        return obj

    def __init__(self, **kwargs):
        kwargs['object_hook'] = self.object_hook
        super(CommonJSONDecoder, self).__init__(**kwargs)


# pylint: disable=too-few-public-methods
class ResourceIterator(object):

    def __init__(self, spec, orig_spec, validate=False):
        self.spec = spec
        self.table_schema = SchemaModel(orig_spec['schema'])
        self.validate = validate

    def __iter__(self):
        return self

    def __next__(self): # pylint: disable=no-self-use
        line = sys.stdin.readline().strip()
        if line == '':
            raise StopIteration()
        # logging.error('INGESTING: {}'.format(line))
        line = json.loads(line, cls=CommonJSONDecoder)
        if self.validate:
            for k, v in line.items():
                try:
                    self.table_schema.cast(k, v)
                except (InvalidCastError, TypeError):
                    field = self.table_schema.get_field(k)
                    if field is None:
                        logging.error('Validation failed: No such field %s', k)
                    else:
                        logging.error('Validation failed: Bad value %r '
                                      'for field %s with type %s',
                                      v, k, field.get('type'))
                    sys.exit(1)
        return line

    def next(self):
        return self.__next__()

cache = ''


def ingest():
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

    dp_json = sys.stdin.readline().strip()
    if dp_json == '':
        logging.error('Missing input')
        sys.exit(1)
    dp = json.loads(dp_json)
    resources = dp.get('resources', [])
    original_resources = copy.deepcopy(resources)

    profiles = list(dp.get('profiles', {}).keys())
    profile = 'tabular'
    if 'tabular' in profiles:
        profiles.remove('tabular')
    if len(profiles) > 0:
        profile = profiles.pop(0)
    schema = datapackage.schema.Schema(profile)
    schema.validate(dp)

    _ = sys.stdin.readline().strip()

    def resources_iterator(_resources, _original_resources):
        # we pass a resource instance that may be changed by the processing
        # code, so we must keep a copy of the original resource (used to
        # validate incoming data)
        for resource, orig_resource in zip(_resources, _original_resources):
            if 'path' not in resource:
                continue

            res_iter = ResourceIterator(resource, orig_resource, validate)
            yield res_iter

    return params, dp, resources_iterator(resources, original_resources)


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
            for f in files:
                f.write(line+'\n')
            # logging.error('SPEWING: {}'.format(line))
            row_count += 1

    logging.info('Processed %d rows', row_count)

    for f in files:
        f.close()

    if len(cache) > 0:
        os.rename(cache_filename+'.ongoing', cache_filename)
