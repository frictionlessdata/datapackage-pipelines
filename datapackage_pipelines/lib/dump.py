import zipfile
import csv
import json
from io import StringIO

from jsontableschema.exceptions import InvalidCastError
from jsontableschema.model import SchemaModel

from datapackage_pipelines.wrapper import ingest, spew


import logging

params, datapackage, res_iter = ingest()

out_filename = open(params['out-file'], 'wb')
out_file = zipfile.ZipFile(out_filename, 'w')

out_file.writestr('datapackage.json', json.dumps(datapackage))


def transform_value(value, _):
    if value is None:
        return ''
    return str(value)


def transform_row(row, fields):
    return dict((k, transform_value(v, fields[k]['type']))
                for k, v in row.items())


def rows_processor(_rows, _csv_file, _zip_file, _writer, _fields, _schema):
    for row in _rows:
        transformed_row = transform_row(row, _fields)
        _writer.writerow(transformed_row)
        for k, v in transformed_row.items():
            try:
                _schema.cast(k, v)
            except InvalidCastError:
                logging.error('Bad value %r for field %s', v, k)
                raise
        yield row
    _zip_file.writestr(_rows.spec['path'], _csv_file.getvalue())


def resource_processor(_res_iter, zip_file):
    for rows in _res_iter:
        schema = rows.spec['schema']

        csv_file = StringIO()
        fields = schema['fields']
        headers = list(map(lambda field: field['name'], fields))

        csv_writer = csv.DictWriter(csv_file, headers)
        csv_writer.writeheader()

        fields = dict((field['name'], field) for field in fields)
        schema = SchemaModel(schema)

        yield rows_processor(rows, csv_file, zip_file, csv_writer, fields, schema)

spew(datapackage, resource_processor(res_iter, out_file))
out_file.close()
