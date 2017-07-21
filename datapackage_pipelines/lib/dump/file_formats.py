import csv
import json
import os

from datapackage_pipelines.utilities.extended_json import DATETIME_FORMAT, DATE_FORMAT, TIME_FORMAT


def identity(x):
    return x


class FileFormat():

    def prepare_resource(self, resource):
        for field in resource.get('schema', {}).get('fields', []):
            field.update(self.PYTHON_DIALECT.get(field['type'], {}))

    def __transform_row(self, row, fields):
        return dict((k, self.__transform_value(v, fields[k]['type']))
                    for k, v in row.items())

    @classmethod
    def __transform_value(cls, value, field_type):
        if value is None:
            return cls.NULL_VALUE
        serializer = cls.SERIALIZERS.get(field_type, cls.DEFAULT_SERIALIZER)
        return serializer(value)

    def write_row(self, writer, row, fields):
        transformed_row = self.__transform_row(row, fields)
        self.write_transformed_row(writer, transformed_row, fields)


class CSVFormat(FileFormat):

    SERIALIZERS = {
        'array': json.dumps,
        'object': json.dumps,
        'datetime': lambda d: d.strftime(DATETIME_FORMAT),
        'date': lambda d: d.strftime(DATE_FORMAT),
        'time': lambda d: d.strftime(TIME_FORMAT),
    }
    DEFAULT_SERIALIZER = str
    NULL_VALUE = ''

    PYTHON_DIALECT = {
        'number': {
            'decimalChar': '.',
            'groupChar': ''
        },
        'date': {
            'format': 'fmt:' + DATE_FORMAT
        },
        'time': {
            'format': 'fmt:' + TIME_FORMAT
        },
        'datetime': {
            'format': 'fmt:' + DATETIME_FORMAT
        },
    }

    def prepare_resource(self, resource):
        resource['encoding'] = 'utf-8'
        basename, _ = os.path.splitext(resource['path'])
        resource['path'] = basename + '.csv'
        resource['format'] = 'csv'
        resource['dialect'] = dict(
            lineTerminator='\r\n',
            delimiter=',',
            doubleQuote=True,
            quoteChar='"',
            skipInitialSpace=False
        )
        super(CSVFormat, self).prepare_resource(resource)

    def initialize_file(self, file, headers):
        csv_writer = csv.DictWriter(file, headers)
        csv_writer.writeheader()
        return csv_writer

    def write_transformed_row(self, writer, transformed_row, fields):
        writer.writerow(transformed_row)

    def finalize_file(self, writer):
        pass


class JSONFormat(FileFormat):

    SERIALIZERS = {
        'datetime': lambda d: d.strftime(DATETIME_FORMAT),
        'date': lambda d: d.strftime(DATE_FORMAT),
        'time': lambda d: d.strftime(TIME_FORMAT),
        'number': float,
    }
    DEFAULT_SERIALIZER = identity
    NULL_VALUE = None

    PYTHON_DIALECT = {
        'date': {
            'format': 'fmt:' + DATE_FORMAT
        },
        'time': {
            'format': 'fmt:' + TIME_FORMAT
        },
        'datetime': {
            'format': 'fmt:' + DATETIME_FORMAT
        },
    }

    def prepare_resource(self, resource):
        resource['encoding'] = 'utf-8'
        basename, _ = os.path.splitext(resource['path'])
        resource['path'] = basename + '.json'
        resource['format'] = 'json'
        super(JSONFormat, self).prepare_resource(resource)

    def initialize_file(self, file, headers):
        writer = file
        writer.write('[')
        writer.__first = True
        return writer

    def write_transformed_row(self, writer, transformed_row, fields):
        if not writer.__first:
            writer.write(',')
        else:
            writer.__first = False
        writer.write(json.dumps(transformed_row, sort_keys=True, ensure_ascii=True))

    def finalize_file(self, writer):
        writer.write(']')
