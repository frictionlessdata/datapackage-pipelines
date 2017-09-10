from datapackage_pipelines.wrapper import ingest, spew

params, datapackage, res_iter = ingest()
for res in datapackage['resources']:
    res['profile'] = 'tabular-data-resource'
spew(datapackage, res_iter)
