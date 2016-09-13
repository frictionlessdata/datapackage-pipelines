import os
import csv
import logging

import itertools
import requests

from jsontableschema.model import SchemaModel

from datapackage_pipelines.wrapper import ingest, spew


def _reader(opener, _url):
    yield None
    filename = os.path.basename(_url)
    _schema, _headers, _csv_reader = opener()
    i = 0
    for i, row in enumerate(_csv_reader):

        row = [x.strip() for x in row]
        if len(row) == 0:
            # response.iter_lines() might emit an empty string here and there
            # if the delimiter is more than one byte:
            #       https://github.com/kennethreitz/requests/pull/2431
            continue
        values = set(row)
        if len(values) == 1 and '' in values:
            # In case of empty rows, just skip them
            continue
        output = dict(
            (header, _schema.cast(header, value))
            for header, value
            in zip(_headers, row)
            if _schema.has_field(header)
        )
        yield output

        i += 1
        if i % 10000 == 0:
            logging.info('%s: %d rows', filename, i)
            # break

    logging.info('%s: TOTAL %d rows', filename, i)


def _null_remover(iterator):
    for line in iterator:
        if line == '':
            continue
        if '\x00' in line:
            continue
        yield line


def csv_stream_reader(_resource, _url, _encoding=None):
    def get_opener(__url, __encoding):
        def opener():
            if __url.startswith('file://'):
                response = open(__url[7:], encoding=__encoding)
            else:
                response = requests.get(__url, stream=True)
                if __encoding is not None:
                    response.encoding = __encoding
                response = response.iter_lines(decode_unicode=True)
            _csv_reader = csv.reader(_null_remover(response))
            _headers = next(_csv_reader)
            _schema = _resource.get('schema')
            if _schema is not None:
                _schema = SchemaModel(_schema)
            return _schema, _headers, _csv_reader
        return opener

    schema, headers, csv_reader = get_opener(url, _encoding)()
    if schema is None:
        schema = {
            'fields': [
                {'name': header, 'type': 'string'}
                for header in headers
                ]
        }
        _resource['schema'] = schema
    del csv_reader

    return itertools\
        .islice(
            _reader(
                get_opener(_url, _encoding),
                _url),
            1, None)


params, datapackage, res_iter = ingest()

new_resource_iterators = []
for resource in datapackage['resources']:
    if 'path' in resource:
        new_resource_iterators.append(next(res_iter))
    elif 'url' in resource and resource.get('mediatype') == 'text/csv':
        url = resource['url']
        basename = os.path.basename(resource['url'])
        path = os.path.join('data', basename)
        del resource['url']
        resource['path'] = path
        encoding = resource.get('encoding')
        rows = csv_stream_reader(resource, url, encoding)
        new_resource_iterators.append(rows)

spew(datapackage, new_resource_iterators)
