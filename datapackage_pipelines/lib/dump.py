import os
import zipfile
import csv
import json
import tempfile
import logging

from jsontableschema.exceptions import InvalidCastError
from jsontableschema.model import SchemaModel

from datapackage_pipelines.wrapper import ingest, spew

params, datapackage, res_iter = ingest()

out_filename = open(params['out-file'], 'wb')
out_file = zipfile.ZipFile(out_filename, 'w')

out_file.writestr('datapackage.json',
                  json.dumps(datapackage, ensure_ascii=True))


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
        for k, v in transformed_row.items():
            try:
                _schema.cast(k, v)
            except InvalidCastError:
                logging.error('Bad value %r for field %s', v, k)
                raise
        _writer.writerow(transformed_row)
        yield row
    filename = _csv_file.name
    _csv_file.close()
    _zip_file.write(filename, arcname=_rows.spec['path'],
                    compress_type=zipfile.ZIP_DEFLATED)
    os.unlink(filename)


def resource_processor(_res_iter, zip_file):
    for rows in _res_iter:
        schema = rows.spec['schema']

        csv_file = tempfile.NamedTemporaryFile(mode="w+", delete=False)
        fields = schema['fields']
        headers = list(map(lambda field: field['name'], fields))

        csv_writer = csv.DictWriter(csv_file, headers)
        csv_writer.writeheader()

        fields = dict((field['name'], field) for field in fields)
        schema = SchemaModel(schema)

        yield rows_processor(rows, csv_file, zip_file,
                             csv_writer, fields, schema)

spew(datapackage, resource_processor(res_iter, out_file))
out_file.close()
