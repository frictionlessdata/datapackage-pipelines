from datapackage_pipelines.wrapper import ingest, spew

params, datapackage, res_iter = ingest()

if 'metadata' in params:
    datapackage.update(params['metadata'])

spew(datapackage, res_iter)
