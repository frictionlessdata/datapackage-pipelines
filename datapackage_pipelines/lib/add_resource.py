from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()


if datapackage is None:
    datapackage = {}

datapackage.setdefault('resources', [])

assert 'path' not in parameters, \
    "You can only add remote resources using this processor"
for param in ['url', 'name']:
    assert param in parameters, \
        "You must define {} in your parameters".format(param)

datapackage['resources'].append(parameters)

spew(datapackage, res_iter)
