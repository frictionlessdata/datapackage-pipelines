import os
import shutil
import tempfile
import logging
import hashlib

import requests
from jsontableschema.exceptions import InvalidCastError
from jsontableschema.model import SchemaModel

from ...utilities.resources import internal_tabular
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

    def __call__(self):
        self.initialize(self.__params)
        self.__datapackage = \
            self.prepare_datapackage(self.__datapackage, self.__params)
        spew(self.__datapackage,
             self.handle_resources(self.__datapackage,
                                   self.__res_iter,
                                   self.__params,
                                   self.stats),
             self.stats)
        self.handle_datapackage(self.__datapackage, self.__params, self.stats)
        self.finalize()

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
        field_names = set([f['name'] for f in schema['fields']])
        schema = SchemaModel(schema)
        warned_fields = set()
        for row in resource:
            for k, v in row.items():
                if k in field_names:
                    try:
                        schema.cast(k, v)
                    except InvalidCastError:
                        logging.error('Bad value %r for field %s', v, k)
                        raise
                else:
                    if k not in warned_fields:
                        warned_fields.add(k)
                        logging.warning('Encountered field %r, not in schema', k)

            yield row

    def row_counter(self, datapackage, resource_spec, resource):
        counter = 0
        for row in resource:
            counter += 1
            if counter % 10000 == 0:
                logging.info('Dumped %d rows', DumperBase.get_attr(datapackage, self.datapackage_rowcount, 0) + counter)
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
                                   sort_keys=True,
                                   ensure_ascii=True).encode('ascii')
                    ).hexdigest()
            DumperBase.set_attr(datapackage, self.datapackage_hash, datapackage_hash)

        stats['count_of_rows'] = DumperBase.get_attr(datapackage, self.datapackage_rowcount)
        stats['bytes'] = DumperBase.get_attr(datapackage, self.datapackage_bytes)
        stats['hash'] = DumperBase.get_attr(datapackage, self.datapackage_hash)
        stats['dataset_name'] = datapackage['name']

    def handle_datapackage(self, datapackage, parameters, stats):
        pass

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
            if not internal_tabular(resource):
                continue
            if force_format:
                file_format = forced_format
            else:
                _, file_format = os.path.splitext(resource['path'])
                file_format = file_format[1:]
            file_formatter = {
                'csv': CSVFormat,
                'json': JSONFormat
            }.get(file_format)
            if file_format is not None:
                self.file_formatters[resource['name']] = file_formatter()
                self.file_formatters[resource['name']].prepare_resource(resource)

        return datapackage

    def handle_datapackage(self, datapackage, parameters, stats):
        temp_file = tempfile.NamedTemporaryFile(mode="w+", delete=False)
        json.dump(datapackage, temp_file, sort_keys=True, ensure_ascii=True)
        temp_file_name = temp_file.name
        temp_file.close()
        self.write_file_to_output(temp_file_name, 'datapackage.json')
        if parameters.get('handle-non-tabular', False):
            self.copy_non_tabular_resources(datapackage)

    def copy_non_tabular_resources(self, datapackage):
        for resource in datapackage['resources']:
            if 'url' in resource and 'path' in resource and 'schema' not in resource:
                url = resource['url']
                delete = False
                if url.startswith('http://') or url.startswith('https://'):
                    tmp = tempfile.NamedTemporaryFile(delete=False)
                    stream = requests.get(url, stream=True).raw
                    shutil.copyfileobj(stream, tmp)
                    tmp.close()
                    url = tmp.name
                    delete = True
                self.write_file_to_output(url, resource['path'])
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
            temp_file.seek(0)
            hasher = hashlib.md5()
            data = 'x'
            while len(data) > 0:
                data = temp_file.read(1024)
                hasher.update(data.encode('utf8'))
            DumperBase.set_attr(spec, self.resource_hash, hasher.hexdigest())

        # Finalise
        filename = temp_file.name
        temp_file.close()
        self.write_file_to_output(filename, spec['path'])

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
