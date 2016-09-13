import logging

from datapackage_pipelines.wrapper import ingest, spew

params, datapackage, res_iter = ingest()

column_aliases = params['column-aliases']
column_mapping = {}
for target, sources in column_aliases.items():
    if sources is not None:
        for source in sources:
            if source in column_mapping:
                logging.error('Duplicate appearance of %s', source)
            assert source not in column_mapping
            column_mapping[source] = target
    if target in column_mapping:
        logging.error('Duplicate appearance of %s', target)
    assert target not in column_mapping
    column_mapping[target] = target

resource_name = params.get('resource-name', 'concat')
concat_resource = {
    'name': resource_name,
    'path': 'data/'+resource_name+'.csv',
    'mediatype': 'text/csv',
    'schema': {
        'fields': [],
        'primaryKey': []
    },
}

used_fields = set()
for resource in datapackage['resources']:
    schema = resource.get('schema', {})
    pk = schema.get('primaryKey', [])
    for field in schema.get('fields', []):
        orig_name = field['name']
        if orig_name in column_mapping:
            name = column_mapping[orig_name]
            if name in used_fields:
                continue
            if orig_name in pk:
                concat_resource['schema']['primaryKey'].append(name)
            concat_resource['schema']['fields'].append(field)
            field['name'] = name
            used_fields.add(name)

datapackage['resources'] = [concat_resource]


def process_resources(_res_iter):
    for rows_iter in _res_iter:
        for row in rows_iter:
            processed = dict((k, '') for k in used_fields)
            values = [(column_mapping[k], v) for (k, v)
                      in row.items()
                      if k in column_mapping]
            assert len(values) > 0
            processed.update(dict(values))
            yield processed

spew(datapackage, [process_resources(res_iter)])
