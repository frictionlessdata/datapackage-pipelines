from datapackage_pipelines.wrapper import ingest, spew

params, datapackage, res_iter = ingest()
spew(datapackage, res_iter)
