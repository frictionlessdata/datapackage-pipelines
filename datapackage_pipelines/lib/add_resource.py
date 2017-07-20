from datapackage_pipelines.wrapper import ingest, spew
import os

parameters, datapackage, res_iter = ingest()


if datapackage is None:
    datapackage = {}

datapackage.setdefault('resources', [])

assert 'path' not in parameters, \
    "You can only add remote resources using this processor"

if parameters['url'].startswith('env://'):
    env_var = parameters['url'][6:]
    env_url = os.environ.get(env_var)
    assert env_url is not None, \
        "Couldn't connect to resource URL - " \
        "Please set your '%s' environment variable" % env_var

    parameters['url'] = env_url

for param in ['url', 'name']:
    assert param in parameters, \
        "You must define {} in your parameters".format(param)

datapackage['resources'].append(parameters)

spew(datapackage, res_iter)
