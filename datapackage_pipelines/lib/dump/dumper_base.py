import os
import csv
import json
import tempfile
import logging

from jsontableschema.exceptions import InvalidCastError
from jsontableschema.model import SchemaModel

from datapackage_pipelines.wrapper import ingest, spew


class DumperBase(object):

    def __init__(self):
        self.__params, self.__datapackage, self.__res_iter = ingest()
        self.stats = {}

    def __call__(self):
        self.initialize(self.__params)
        self.__datapackage = self.handle_datapackage(self.__datapackage, self.__params)
        spew(self.__datapackage,
             self.handle_resources(self.__datapackage, self.__res_iter, self.__params),
             self.stats)
        self.finalize()

    def handle_datapackage(self, datapackage, params):
        self.stats['total_row_count'] = 0
        self.stats['dataset_name'] = datapackage['name']
        return datapackage

    def schema_validator(self, resource):
        schema = SchemaModel(resource.spec['schema'])
        for row in resource:
            for k, v in row.items():
                try:
                    schema.cast(k, v)
                except InvalidCastError:
                    logging.error('Bad value %r for field %s', v, k)
                    raise
            yield row

    def row_counter(self, resource):
        for row in resource:
            self.stats['total_row_count'] += 1
            if self.stats['total_row_count'] % 1 == 10000:
                logging.info('Dumped %d rows', self.stats['total_row_count'])
            yield row

    def handle_resources(self, datapackage, resource_iterator, parameters):
        for resource in resource_iterator:
            resource_spec = resource.spec
            yield self.row_counter(self.handle_resource(self.schema_validator(resource),
                                                        resource_spec,
                                                        parameters,
                                                        datapackage))

    def handle_resource(self, resource, spec, parameters, datapackage):
        raise NotImplementedError()

    def initialize(self, params):
        pass

    def finalize(self):
        pass


class CSVDumper(DumperBase):

    def handle_datapackage(self, datapackage, params):
        datapackage = super(CSVDumper, self).handle_datapackage(datapackage, params)

        # Make sure all resources are proper CSVs
        for resource in datapackage['resources']:
            resource['encoding'] = 'utf-8'
            basename, extension = os.path.splitext(resource['path'])
            resource['path'] = basename + '.csv'

        temp_file = tempfile.NamedTemporaryFile(mode="w+", delete=False)
        json.dump(datapackage, temp_file, sort_keys=True, ensure_ascii=True)
        temp_file_name = temp_file.name
        temp_file.close()
        self.write_file_to_output(temp_file_name, 'datapackage.json')

        return datapackage

    def write_file_to_output(self, filename, path):
        raise NotImplementedError()

    def rows_processor(self, resource, spec, _csv_file, _writer, _fields):
        for row in resource:
            transformed_row = CSVDumper.__transform_row(row, _fields)
            _writer.writerow(transformed_row)
            yield row
        filename = _csv_file.name
        _csv_file.close()
        self.write_file_to_output(filename, spec['path'])

    def handle_resource(self, resource, spec, params, datapackage):
        schema = spec['schema']

        temp_file = tempfile.NamedTemporaryFile(mode="w+", delete=False)
        fields = schema['fields']
        headers = list(map(lambda field: field['name'], fields))

        csv_writer = csv.DictWriter(temp_file, headers)
        csv_writer.writeheader()

        fields = dict((field['name'], field) for field in fields)

        return self.rows_processor(resource, spec, temp_file, csv_writer, fields)

    @staticmethod
    def __transform_value(value, _):
        if value is None:
            return ''
        return str(value)

    @staticmethod
    def __transform_row(row, fields):
        return dict((k, CSVDumper.__transform_value(v, fields[k]['type']))
                    for k, v in row.items())


