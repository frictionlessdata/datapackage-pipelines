import functools
import os
import shutil
import tempfile
import logging
import hashlib
import copy
import importlib

import requests
from tableschema.exceptions import CastError
from tableschema.schema import Schema

from ...utilities.stat_utils import STATS_DPP_KEY, STATS_OUT_DP_URL_KEY
from ...utilities.resources import get_path, PROP_STREAMED_FROM, PROP_STREAMING, is_a_url, streaming
from ...utilities.extended_json import json
from ...wrapper import ingest, spew

from .file_formats import CSVFormat, JSONFormat


class DumperBase(object):

    def __init__(self, debug=False):
        self.__params, self.__datapackage, self.__res_iter = ingest(debug)
        self.stats = {}
        counters = self.__params.get('counters', {})
        self.datapackage_rowcount = counters.get('datapackage-rowcount', 'count_of_rows')
        self.datapackage_bytes = counters.get('datapackage-bytes', 'bytes')
        self.datapackage_hash = counters.get('datapackage-hash', 'hash')
        self.resource_rowcount = counters.get('resource-rowcount', 'count_of_rows')
        self.resource_bytes = counters.get('resource-bytes', 'bytes')
        self.resource_hash = counters.get('resource-hash', 'hash')
        self.add_filehash_to_path = self.__params.get('add-filehash-to-path', False)
        self.file_format_handlers = {
            'csv': CSVFormat,
            'json': JSONFormat
        }
        self.file_format_handlers.update(**self.__params.get('file-formatters', {}))

    def __call__(self):
        self.initialize(self.__params)
        self.__datapackage = \
            self.prepare_datapackage(self.__datapackage, self.__params)

        spew(
            self.__datapackage,
            self.handle_resources(self.__datapackage,
                                  self.__res_iter,
                                  self.__params,
                                  self.stats),
            self.stats,
            finalizer=self.finalize
        )

    def prepare_datapackage(self, datapackage, _):
        return datapackage

    @staticmethod
    def get_attr(obj, prop, default=None):
        if prop is None:
            return
        prop = prop.split('.')
        while len(prop) > 1:
            obj = obj.get(prop.pop(0), {})
        prop = prop.pop(0)
        return obj.get(prop, default)

    @staticmethod
    def set_attr(obj, prop, value):
        if prop is None:
            return
        prop = prop.split('.')
        while len(prop) > 1:
            obj = obj.setdefault(prop.pop(0), {})
        prop = prop.pop(0)
        obj[prop] = value

    @staticmethod
    def inc_attr(obj, prop, value):
        if prop is None:
            return
        prop = prop.split('.')
        while len(prop) > 1:
            obj = obj.setdefault(prop.pop(0), {})
        prop = prop.pop(0)
        obj.setdefault(prop, 0)
        obj[prop] += value

    @staticmethod
    def schema_validator(resource):
        schema = resource.spec['schema']
        field_names = [f['name'] for f in schema['fields']]
        schema = Schema(schema)
        warned_fields = set()
        for row in resource:
            to_cast = [row.get(f) for f in field_names]
            try:
                schema.cast_row(to_cast)
            except CastError as e:
                logging.error('Failed to cast row %r', row)
                for i, err in enumerate(e.errors):
                    logging.error('%d) %s', i+1, err)
                raise ValueError(row) from e

            for k in set(row.keys()) - set(field_names):
                if k not in warned_fields:
                    warned_fields.add(k)
                    logging.warning('Encountered field %r, not in schema', k)

            yield row

    @staticmethod
    def insert_hash_in_path(descriptor, hash):
        path = descriptor.get('path')
        if isinstance(path, list):
            if len(path) > 0:
                path = path[0]

        assert isinstance(path, str), '%r' % path

        dir_name = os.path.dirname(path)
        file_name = os.path.basename(path)
        descriptor['path'] = os.path.join(dir_name, hash, file_name)

    def row_counter(self, datapackage, resource_spec, resource):
        counter = 0
        for row in resource:
            counter += 1
            yield row
        DumperBase.inc_attr(datapackage, self.datapackage_rowcount, counter)
        DumperBase.inc_attr(resource_spec, self.resource_rowcount, counter)

    def handle_resources(self, datapackage,
                         resource_iterator,
                         parameters, stats):
        for resource in resource_iterator:
            resource_spec = resource.spec
            ret = self.handle_resource(DumperBase.schema_validator(resource),
                                       resource_spec,
                                       parameters,
                                       datapackage)
            ret = self.row_counter(datapackage, resource_spec, ret)
            yield ret

        # Calculate datapackage hash
        if self.datapackage_hash:
            datapackage_hash = hashlib.md5(
                        json.dumps(datapackage,
                                   indent=2 if parameters.get('pretty-descriptor') else None,
                                   sort_keys=True,
                                   ensure_ascii=True).encode('ascii')
                    ).hexdigest()
            DumperBase.set_attr(datapackage, self.datapackage_hash, datapackage_hash)

        self.handle_datapackage(datapackage, parameters, stats)

    def handle_datapackage(self, datapackage, parameters, stats):
        stats['count_of_rows'] = DumperBase.get_attr(datapackage, self.datapackage_rowcount)
        stats['bytes'] = DumperBase.get_attr(datapackage, self.datapackage_bytes)
        stats['hash'] = DumperBase.get_attr(datapackage, self.datapackage_hash)
        stats['dataset_name'] = datapackage['name']

    def handle_resource(self, resource, spec, parameters, datapackage):
        raise NotImplementedError()

    def initialize(self, params):
        pass

    def finalize(self):
        pass


class FileDumper(DumperBase):

    def prepare_datapackage(self, datapackage, params):
        datapackage = \
            super(FileDumper, self).prepare_datapackage(datapackage, params)

        force_format = params.get('force-format', True)
        forced_format = params.get('format', 'csv')

        self.file_formatters = {}

        # Make sure all resources are proper CSVs
        for resource in datapackage['resources']:
            if not streaming(resource):
                continue
            if force_format:
                file_format = forced_format
            else:
                _, file_format = os.path.splitext(get_path(resource))
                file_format = file_format[1:]
            file_formatter = self.file_format_handlers.get(file_format)
            if isinstance(file_formatter, str):
                file_formatter = file_formatter.rsplit('.', 1)
                file_formatter = getattr(importlib.import_module(file_formatter[0]), file_formatter[1])
            if file_format is not None:
                self.file_formatters[resource['name']] = file_formatter()
                self.file_formatters[resource['name']].prepare_resource(resource)

        return datapackage

    def handle_datapackage(self, datapackage, parameters, stats):
        if parameters.get('handle-non-tabular', False):
            self.copy_non_tabular_resources(datapackage)
        datapackage_copy = copy.deepcopy(datapackage)
        for res in datapackage_copy['resources']:
            if PROP_STREAMING in res:
                del res[PROP_STREAMING]
        temp_file = tempfile.NamedTemporaryFile(mode="w+", delete=False, encoding='utf-8')
        indent = 2 if parameters.get('pretty-descriptor') else None
        json.dump(datapackage_copy, temp_file, indent=indent, sort_keys=True, ensure_ascii=False)
        temp_file_name = temp_file.name
        filesize = temp_file.tell()
        temp_file.close()
        DumperBase.inc_attr(datapackage, self.datapackage_bytes, filesize)
        location = self.write_file_to_output(temp_file_name, 'datapackage.json')
        if location is not None:
            stats.setdefault(STATS_DPP_KEY, {})[STATS_OUT_DP_URL_KEY] = location
        os.unlink(temp_file_name)
        super(FileDumper, self).handle_datapackage(datapackage, parameters, stats)

    def copy_non_tabular_resources(self, datapackage):
        for resource in datapackage['resources']:
            if not streaming(resource):
                url = resource[PROP_STREAMED_FROM]
                delete = False
                if is_a_url(url):
                    tmp = tempfile.NamedTemporaryFile(delete=False)
                    stream = requests.get(url, stream=True).raw
                    stream.read = functools.partial(stream.read, decode_content=True)
                    shutil.copyfileobj(stream, tmp)
                    filesize = tmp.tell()
                    if self.add_filehash_to_path:
                        hasher = FileDumper.hash_handler(tmp)
                        DumperBase.insert_hash_in_path(resource, hasher.hexdigest())
                    tmp.close()
                    url = tmp.name
                    delete = True
                else:
                    if self.add_filehash_to_path:
                        hasher = FileDumper.hash_handler(open(url, 'rb'))
                        DumperBase.insert_hash_in_path(resource, hasher.hexdigest())
                    filesize = os.stat(url).st_size
                DumperBase.set_attr(resource, self.resource_bytes, filesize)
                DumperBase.inc_attr(datapackage, self.datapackage_bytes, filesize)
                self.write_file_to_output(url, get_path(resource))
                if delete:
                    os.unlink(url)

    def write_file_to_output(self, filename, path):
        raise NotImplementedError()

    def rows_processor(self, resource, spec, temp_file, writer, fields, datapackage):
        file_formatter = self.file_formatters[spec['name']]
        for row in resource:
            file_formatter.write_row(writer, row, fields)
            yield row
        file_formatter.finalize_file(writer)

        # File size:
        filesize = temp_file.tell()
        DumperBase.inc_attr(datapackage, self.datapackage_bytes, filesize)
        DumperBase.inc_attr(spec, self.resource_bytes, filesize)

        # File Hash:
        if self.resource_hash:
            hasher = FileDumper.hash_handler(temp_file)
            # Update path with hash
            if self.add_filehash_to_path:
                DumperBase.insert_hash_in_path(spec, hasher.hexdigest())
            DumperBase.set_attr(spec, self.resource_hash, hasher.hexdigest())

        # Finalise
        filename = temp_file.name
        temp_file.close()
        self.write_file_to_output(filename, get_path(spec))
        os.unlink(filename)

    def handle_resource(self, resource, spec, _, datapackage):
        if spec['name'] in self.file_formatters:
            schema = spec['schema']

            temp_file = tempfile.NamedTemporaryFile(mode="w+", delete=False)
            fields = schema['fields']
            headers = list(map(lambda field: field['name'], fields))

            writer = self.file_formatters[spec['name']].initialize_file(temp_file, headers)

            fields = dict((field['name'], field) for field in fields)

            return self.rows_processor(resource,
                                       spec,
                                       temp_file,
                                       writer,
                                       fields,
                                       datapackage)
        else:
            return resource

    @staticmethod
    def hash_handler(tfile):
        tfile.seek(0)
        hasher = hashlib.md5()
        data = 'x'
        while len(data) > 0:
            data = tfile.read(1024)
            if isinstance(data, str):
                hasher.update(data.encode('utf8'))
            elif isinstance(data, bytes):
                hasher.update(data)
        return hasher
