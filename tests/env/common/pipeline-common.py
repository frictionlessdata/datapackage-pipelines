from datapackage_pipelines.wrapper import ingest, spew

params, datapackage, res_iter = ingest()
datapackage['profile'] = 'tabular-data-package'
spew(datapackage, res_iter)
