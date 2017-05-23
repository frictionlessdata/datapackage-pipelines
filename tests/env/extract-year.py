from datapackage_pipelines.wrapper import ingest, spew

params, datapackage, res_iter = ingest()

from_key = params['from-key']
to_key = params['to-key']


def process_resources(_res_iter):
    for res in _res_iter:
        def process_res(_res):
            for line in _res:
                if from_key in line:
                    line[to_key] = line[from_key].year
                    yield line
    yield process_res(res)


for resource in datapackage['resources']:
    if len(list(filter(lambda field: field['name'] == from_key, resource.get('schema',{}).get('fields',[])))) > 0:
        resource['schema']['fields'].append({
            'name': to_key,
            'osType': 'date:fiscal-year',
            'type': 'integer'
        })

spew(datapackage, process_resources(res_iter))
