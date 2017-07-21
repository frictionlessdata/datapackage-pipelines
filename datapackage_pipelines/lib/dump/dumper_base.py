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

    def __init__(self):
        self.__params, self.__datapackage, self.__res_iter = ingest()
        self.stats = {}

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
    def schema_validator(resource):
        schema = SchemaModel(resource.spec['schema'])
        for row in resource:
            for k, v in row.items():
                try:
                    schema.cast(k, v)
                except InvalidCastError:
                    logging.error('Bad value %r for field %s', v, k)
                    raise
                except TypeError:
                    logging.error('Failed to cast value %r for field %s, possibly missing from schema', v, k)
                    raise

            yield row

    @staticmethod
    def row_counter(datapackage, resource_spec, resource):
        resource_spec['count_of_rows'] = 0
        for row in resource:
            datapackage['count_of_rows'] += 1
            resource_spec['count_of_rows'] += 1
            if datapackage['count_of_rows'] % 1 == 10000:
                logging.info('Dumped %d rows', datapackage['count_of_rows'])
            yield row

    @staticmethod
    def hasher(datapackage, resource_spec, resource):
        resource_spec['hash'] = hashlib.md5()
        for row in resource:
            row_dump = json.dumps(row,
                                  sort_keys=True,
                                  ensure_ascii=True)\
                           .encode('utf8')
            resource_spec['hash'].update(row_dump)
            datapackage['hash'].update(row_dump)
            yield row
        resource_spec['hash'] = resource_spec['hash'].hexdigest()

    def handle_resources(self, datapackage,
                         resource_iterator,
                         parameters, stats):
        datapackage['count_of_rows'] = 0
        datapackage['hash'] = hashlib.md5()
        for resource in resource_iterator:
            resource_spec = resource.spec
            ret = self.handle_resource(DumperBase.schema_validator(resource),
                                       resource_spec,
                                       parameters,
                                       datapackage)
            ret = DumperBase.row_counter(datapackage, resource_spec, ret)
            ret = DumperBase.hasher(datapackage, resource_spec, ret)
            yield ret

        datapackage['hash'] = datapackage['hash'].hexdigest()
        stats['count_of_rows'] = datapackage['count_of_rows']
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

        file_format = params.get('format', 'csv')
        self.file_format = {
            'csv': CSVFormat,
            'json': JSONFormat
        }[file_format]()

        # Make sure all resources are proper CSVs
        for resource in datapackage['resources']:
            if not internal_tabular(resource):
                continue
            self.file_format.prepare_resource(resource)

        return datapackage

    def handle_datapackage(self, datapackage, parameters, stats):
        temp_file = tempfile.NamedTemporaryFile(mode="w+", delete=False)
        json.dump(datapackage, temp_file, sort_keys=True, ensure_ascii=True)
        temp_file_name = temp_file.name
        temp_file.close()
        self.write_file_to_output(temp_file_name, 'datapackage.json')
        self.copy_non_tabular_resources(datapackage)

    def copy_non_tabular_resources(self, datapackage):
        for resource in datapackage['resources']:
            if 'url' in resource and 'path' in resource and 'schema' not in resource:
                url = resource['url']
                delete = False
                if url.startswith('http://') or url.startswith('https://'):
                    tmp = tempfile.NamedTemporaryFile(delete=False)
                    stream = requests.get(url, stream=True)
                    shutil.copyfileobj(stream, tmp)
                    tmp.close()
                    url = tmp.name
                    delete = True
                self.write_file_to_output(url, resource['path'])
                if delete:
                    os.unlink(url)

    def write_file_to_output(self, filename, path):
        raise NotImplementedError()

    def rows_processor(self, resource, spec, temp_file, writer, fields):
        for row in resource:
            self.file_format.write_row(writer, row, fields)
            yield row
        self.file_format.finalize_file(writer)
        filename = temp_file.name
        temp_file.close()
        self.write_file_to_output(filename, spec['path'])

    def handle_resource(self, resource, spec, _, datapackage):
        schema = spec['schema']

        temp_file = tempfile.NamedTemporaryFile(mode="w+", delete=False)
        fields = schema['fields']
        headers = list(map(lambda field: field['name'], fields))

        writer = self.file_format.initialize_file(temp_file, headers)

        fields = dict((field['name'], field) for field in fields)

        return self.rows_processor(resource,
                                   spec,
                                   temp_file,
                                   writer,
                                   fields)
