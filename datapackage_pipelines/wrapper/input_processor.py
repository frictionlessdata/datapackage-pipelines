import sys
import copy
import logging

import datapackage
from tableschema.exceptions import ValidationError, CastError
from tableschema import Schema

from ..utilities.resources import PATH_PLACEHOLDER, streaming
from ..utilities.extended_json import json


class ResourceIterator(object):

    def __init__(self, infile, spec, orig_spec,
                 validate=False, debug=False):
        self.spec = spec
        self.table_schema = Schema(orig_spec['schema'])
        self.field_names = [f['name'] for f in orig_spec['schema']['fields']]
        self.validate = validate
        self.infile = infile
        self.debug = debug
        self.stopped = False

    def __iter__(self):
        return self

    def __next__(self):
        if self.stopped:
            raise StopIteration()
        if self.debug:
            logging.error('WAITING')
        line = self.infile.readline().strip()
        if self.debug:
            logging.error('INGESTING: %r', line)
        if line == '':
            self.stopped = True
            raise StopIteration()
        line = json.loadl(line)
        if self.validate:
            to_validate = [line.get(f) for f in self.field_names]
            try:
                self.table_schema.cast_row(to_validate)
            except CastError as e:
                logging.error('Failed to validate row: %s', e)
                for i, err in enumerate(e.errors):
                    logging.error('%d) %s', i+1, err.message)
                raise ValueError('Casting failed for row %r' % line) from e
            except TypeError as e:
                raise ValueError('Validation failed for row %r' % line) from e

        return line

    def next(self):
        return self.__next__()


def read_json(infile, proxy=False):
    line_json = infile.readline().strip()
    if line_json == '':
        sys.exit(-3)
    if proxy:
        print(line_json)
    try:
        return json.loads(line_json)
    except json.JSONDecodeError:
        logging.exception("Failed to decode line %r\nPerhaps there's a rogue print statement somewhere?", line_json)


def process_input(infile, validate=False, debug=False):
    dependency_dp = read_json(infile, True)
    dp = read_json(infile)
    resources = dp.get('resources', [])
    original_resources = copy.deepcopy(resources)

    if len(dp.get('resources', [])) == 0:
        # Currently datapackages with no resources are disallowed in the schema.
        # Since this might happen in the early stages of a pipeline,
        # we're adding this hack to avoid validation errors
        dp_to_validate = copy.deepcopy(dp)
        dp_to_validate['resources'] = [{
            'name': '__placeholder__',
            'path': PATH_PLACEHOLDER
        }]
    else:
        dp_to_validate = dp
    try:
        datapackage.validate(dp_to_validate)
    except ValidationError as e:
        logging.info('FAILED TO VALIDATE %r', dp_to_validate)
        for e in e.errors:
            try:
                logging.error("Data Package validation error: %s at dp%s",
                              e.message,
                              "[%s]" % "][".join(repr(index) for index in e.path))
            except AttributeError:
                logging.error("Data Package validation error: %s", e)
        raise

    infile.readline().strip()

    def resources_iterator(_resources, _original_resources):
        # we pass a resource instance that may be changed by the processing
        # code, so we must keep a copy of the original resource (used to
        # validate incoming data)
        ret = []
        for resource, orig_resource in zip(_resources, _original_resources):
            if not streaming(resource):
                continue

            res_iter = ResourceIterator(infile,
                                        resource, orig_resource,
                                        validate, debug)
            ret.append(res_iter)
        return iter(ret)

    return dp, resources_iterator(resources, original_resources), dependency_dp
