from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()
if datapackage is None:
    datapackage = parameters
else:
    datapackage.update(parameters)

spew(datapackage, res_iter)
