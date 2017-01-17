#import logging
from datapackage_pipelines.wrapper import ingest, spew

params, datapackage, res_iter = ingest()
#logging.info(datapackage)

fields = datapackage['resources'][0]['schema']['fields']
del fields[:]
fields.append({'name': 'Budget Institution', 'osType': 'administrator:generic:name', 'type': 'string'})
fields.append({'name': 'Supplier', 'osType': 'supplier:generic:name', 'type': 'string'})
fields.append({'name': 'Treasury Branch', 'osType': 'unknown:string', 'type': 'string'})
fields.append({'name': 'Value', 'osType': 'value', 'type': 'number'})
fields.append({'name': 'Date registered', 'osType': 'date:generic', 'type': 'date'})
fields.append({'name': 'Date executed', 'osType': 'date:generic', 'type': 'date'})
fields.append({'name': 'Receipt No', 'osType': 'transaction-id:code', 'type': 'string'})
fields.append({'name': 'Kategori Shpenzimi', 'osType': 'unknown:string', 'type': 'string'})
fields.append({'name': 'Receipt Description', 'osType': 'unknown:string', 'type': 'string'})


def process_resources(_res_iter):
#  logging.info(_res_iter)
  yield _res_iter

spew(datapackage, process_resources(res_iter))
