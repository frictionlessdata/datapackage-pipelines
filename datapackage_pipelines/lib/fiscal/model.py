import json
import logging
import subprocess

from datapackage_pipelines.wrapper import ingest, spew

params, datapackage, res_iter = ingest()

os_types = params['os-types']
options = params['options']

resource = datapackage['resources'][0]
fields = resource['schema']['fields']

for field in fields:
    field_name = field['name']
    if field_name not in os_types:
        logging.error('Missing OS Type for field %s', field_name)
    field['type'] = os_types[field_name]
    field['options'] = options.get(field_name, {})

result = subprocess.run(['/usr/bin/env', 'os-types', json.dumps(fields)],
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)

errors = result.stderr.decode('utf8')
if len(errors) > 0:
    raise RuntimeError(errors)
output = result.stdout.decode('utf8')
if output.startswith('FAILED'):
    raise RuntimeError(output)

model = json.loads(output)

datapackage['model'] = model['model']

for field in fields:
    field.update(model['schema']['fields'][field['name']])
    if 'options' in field:
        del field['options']

resource['schema']['primaryKey'] = model['schema']['primaryKey']

datapackage['profiles'] = {
    'fiscal': '*',
    'tabular': '*'
}
datapackage['@context'] = 'http://schemas.frictionlessdata.io/fiscal-data-package.jsonld'

spew(datapackage, res_iter)
