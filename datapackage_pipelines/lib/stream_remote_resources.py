import os
import logging
import time
from datetime import date
import itertools
from decimal import Decimal

import tabulator

from tableschema import Schema

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import streamable, PATH_PLACEHOLDER, get_path, \
    PROP_STREAMED_FROM, PROP_STREAMING, streaming
from datapackage_pipelines.utilities.extended_json import json
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher
from datapackage_pipelines.utilities.tabulator_txt_parser import TXTParser


def _tostr(value):
    if isinstance(value, str):
        return value
    elif value is None:
        return ''
    elif isinstance(value, (int, float, bool, Decimal)):
        return str(value)
    elif isinstance(value, date):
        return value.isoformat()
    elif isinstance(value, (list, dict)):
        return json.dumps(value)

    assert False, "Internal error - don't know how to handle %r of type %r" % (value, type(value))


def _reader(opener, _url, max_row=-1):
    yield None
    filename = os.path.basename(_url)
    logging.info('%s: OPENING %s', filename, _url)
    _schema, _headers, _reader, _close = opener()
    num_headers = len(_headers)
    i = 0
    for i, row in enumerate(_reader):
        row = [_tostr(x).strip() for x in row]
        values = set(row)
        if len(values) == 1 and '' in values:
            # In case of empty rows, just skip them
            continue
        output = dict(zip(_headers, _schema.cast_row(row[:num_headers])))
        yield output

        i += 1
        if i % 10000 == 0:
            logging.info('%s: %d rows', filename, i)
            # break
        if i == max_row:
            break

    _close()
    logging.info('%s: TOTAL %d rows', filename, i)


def dedupe(headers):
    _dedupped_headers = []
    headers = list(map(str, headers))
    for hdr in headers:
        if hdr is None:
            continue
        hdr = hdr.strip()
        if len(hdr) == 0:
            continue
        if hdr in _dedupped_headers:
            i = 0
            deduped_hdr = hdr
            while deduped_hdr in _dedupped_headers:
                i += 1
                deduped_hdr = '%s_%s' % (hdr, i)
            hdr = deduped_hdr
        _dedupped_headers.append(hdr)
    return _dedupped_headers


def add_constants(extra_headers, extra_values):
    def _func(extended_rows):
        for number, headers, row in extended_rows:
            row = row[:len(headers)] + extra_values
            yield number, headers + extra_headers, row
    return _func


def suffix_remover(format):
    def empty_row(row):
        return all(v is None or (isinstance(v, str) and v == '')
                   for v in row)

    def _func(extended_rows):
        suffix = False
        for number, headers, row in extended_rows:
            if format == 'txt':
                yield number, headers, row
                continue

            if suffix:
                if len(row) >= len(headers):
                    logging.warning('Expected an empty row, but got %r instead' % row)
            else:
                if empty_row(row):
                    continue
                elif len(row) < len(headers):
                    suffix = True
                else:
                    yield number, headers, row

    return _func


def stream_reader(_resource, _url, _ignore_missing, limit_rows):
    def get_opener(__url, __resource):
        def opener():
            _params = dict(headers=1)
            format = __resource.get("format")
            if format == "txt":
                # datapackage-pipelines processing requires having a header row
                # for txt format we add a single "data" column
                _params["headers"] = ["data"]
                _params["custom_parsers"] = {"txt": TXTParser}
                _params["allow_html"] = True
            else:
                if format is None:
                    _, format = tabulator.helpers.detect_scheme_and_format(__url)
                if format in tabulator.config.SUPPORTED_COMPRESSION:
                    format = None
                else:
                    try:
                        parser_cls = tabulator.helpers.import_attribute(tabulator.config.PARSERS[format])
                    except KeyError:
                        logging.error("Unknown format %r", format)
                        raise
                    _params.update(
                        dict(x for x in __resource.items()
                             if x[0] in parser_cls.options))
                _params.update(
                    dict(x for x in __resource.items()
                         if x[0] in {'headers', 'scheme', 'encoding', 'sample_size', 'allow_html',
                                     'force_strings', 'force_parse', 'skip_rows', 'compression'}))
                if isinstance(_params.get('skip_rows'), int):  # Backwards compatibility
                    _params['skip_rows'] = list(range(1, _params.get('skip_rows') + 1))

            if format is not None:
                _params['format'] = format

            constants = _resource.get('constants', {})
            constant_headers = list(constants.keys())
            constant_values = [constants.get(k) for k in constant_headers]
            _stream = tabulator.Stream(__url, **_params,
                                       post_parse=[suffix_remover(format),
                                                   add_constants(constant_headers, constant_values)])
            retry = 0
            backoff = 2
            while True:
                try:
                    _stream.open()
                    _headers = dedupe(_stream.headers + constant_headers)
                    _schema = __resource.get('schema')
                    if _schema is not None:
                        _schema = Schema(_schema)
                    return _schema, _headers, _stream, _stream.close
                except tabulator.exceptions.TabulatorException as e:
                    logging.warning("Error while opening resource from url %s: %r",
                                    _url, e)
                    _stream.close()
                    retry += 1
                    if retry <= 3:
                        logging.warning("Retrying after %d seconds (%d/3)", backoff, retry)
                        time.sleep(backoff)
                        backoff *= 2
                        continue
                    else:
                        if not _ignore_missing:
                            raise
                        return {}, [], [], lambda: None
        return opener

    schema, headers, stream, close = get_opener(url, _resource)()
    if schema is None:
        schema = {
            'fields': [
                {'name': header, 'type': 'string'}
                for header in headers
                ]
        }
        _resource['schema'] = schema

    close()
    del stream

    return itertools\
        .islice(
            _reader(
                get_opener(_url, _resource),
                _url,
                max_row=limit_rows),
            1, None)


parameters, datapackage, resource_iterator = ingest()

resources = ResourceMatcher(parameters.get('resources'))
ignore_missing = parameters.get('ignore-missing', False)
limit_rows = parameters.get('limit-rows', -1)

new_resource_iterator = []
for resource in datapackage['resources']:

    if streamable(resource):
        url = resource[PROP_STREAMED_FROM]

        name = resource['name']
        if not resources.match(name):
            continue

        path = get_path(resource)
        if path is None or path == PATH_PLACEHOLDER:
            path = os.path.join('data', name + '.csv')
            resource['path'] = path

        resource[PROP_STREAMING] = True

        rows = stream_reader(resource, url, ignore_missing or url == "", limit_rows)

        new_resource_iterator.append(rows)

    elif streaming(resource):
        new_resource_iterator.append(next(resource_iterator))

spew(datapackage, new_resource_iterator)
