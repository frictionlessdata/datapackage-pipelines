import csv
import json
import os
import isodate
import logging

from datapackage_pipelines.utilities.extended_json import DATETIME_FORMAT, DATE_FORMAT, TIME_FORMAT
from datapackage_pipelines.utilities.resources import get_path


def identity(x):
    return x


def json_dumps(x):
    return json.dumps(x, ensure_ascii=False)


class FileFormat():

    def prepare_resource(self, resource):
        for field in resource.get('schema', {}).get('fields', []):
            field.update(self.PYTHON_DIALECT.get(field['type'], {}))

    def __transform_row(self, row, fields):
        try:
            return dict((k, self.__transform_value(v, fields[k]['type']))
                        for k, v in row.items())
        except Exception:
            logging.exception('Failed to transform row %r', row)
            raise

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
        'array': json_dumps,
        'object': json_dumps,
        'datetime': lambda d: d.strftime(DATETIME_FORMAT),
        'date': lambda d: d.strftime(DATE_FORMAT),
        'time': lambda d: d.strftime(TIME_FORMAT),
        'duration': lambda d: isodate.duration_isoformat(d),
        'geopoint': lambda d: '{}, {}'.format(*d),
        'geojson': json.dumps,
        'year': lambda d: '{:04d}'.format(d),
        'yearmonth': lambda d: '{:04d}-{:02d}'.format(*d),
    }
    DEFAULT_SERIALIZER = str
    NULL_VALUE = ''

    PYTHON_DIALECT = {
        'number': {
            'decimalChar': '.',
            'groupChar': ''
        },
        'date': {
            'format': DATE_FORMAT
        },
        'time': {
            'format': TIME_FORMAT
        },
        'datetime': {
            'format': DATETIME_FORMAT
        },
    }

    def prepare_resource(self, resource):
        resource['encoding'] = 'utf-8'
        basename, _ = os.path.splitext(get_path(resource))
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
        'duration': lambda d: isodate.duration_isoformat(d),
        'geopoint': lambda d: list(map(float, d)),
        'yearmonth': lambda d: '{:04d}-{:02d}'.format(*d),
    }
    DEFAULT_SERIALIZER = identity
    NULL_VALUE = None

    PYTHON_DIALECT = {
        'date': {
            'format': DATE_FORMAT
        },
        'time': {
            'format': TIME_FORMAT
        },
        'datetime': {
            'format': DATETIME_FORMAT
        },
    }

    def prepare_resource(self, resource):
        resource['encoding'] = 'utf-8'
        basename, _ = os.path.splitext(get_path(resource))
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
