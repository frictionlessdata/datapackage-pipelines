import datetime
import json
import copy
import decimal
import logging

import datapackage
from jsontableschema.exceptions import InvalidCastError

from jsontableschema.model import SchemaModel


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


# pylint: disable=too-few-public-methods, too-many-arguments
class ResourceIterator(object):

    def __init__(self, infile, spec, orig_spec,
                 validate=False, debug=False):
        self.spec = spec
        self.table_schema = SchemaModel(orig_spec['schema'])
        self.validate = validate
        self.infile = infile
        self.debug = debug

    def __iter__(self):
        return self

    def __next__(self):
        if self.debug:
            logging.error('WAITING')
        line = self.infile.readline().strip()
        if self.debug:
            logging.error('INGESTING: %r', line)
        if line == '':
            raise StopIteration()
        line = json.loads(line, cls=CommonJSONDecoder)
        if self.validate:
            for k, v in line.items():
                try:
                    self.table_schema.cast(k, v)
                except (InvalidCastError, TypeError):
                    field = self.table_schema.get_field(k)
                    if field is None:
                        raise ValueError('Validation failed: No such field %s',
                                         k)
                    else:
                        raise ValueError('Validation failed: Bad value %r '
                                         'for field %s with type %s',
                                         v, k, field.get('type'))

        return line

    def next(self):
        return self.__next__()


def process_input(infile, validate=False, debug=False):

    dp_json = infile.readline().strip()
    if dp_json == '':
        raise ValueError('Missing input')
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

    _ = infile.readline().strip()

    def resources_iterator(_resources, _original_resources):
        # we pass a resource instance that may be changed by the processing
        # code, so we must keep a copy of the original resource (used to
        # validate incoming data)
        for resource, orig_resource in zip(_resources, _original_resources):
            if 'path' not in resource:
                continue

            res_iter = ResourceIterator(infile,
                                        resource, orig_resource,
                                        validate, debug)
            yield res_iter

    return dp, resources_iterator(resources, original_resources)
