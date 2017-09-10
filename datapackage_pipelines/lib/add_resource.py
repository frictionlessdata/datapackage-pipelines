from datapackage_pipelines.wrapper import ingest, spew
import os

from datapackage_pipelines.utilities.resources import PATH_PLACEHOLDER, PROP_STREAMED_FROM

parameters, datapackage, res_iter = ingest()


if datapackage is None:
    datapackage = {}

datapackage.setdefault('resources', [])

for param in ['url', 'name']:
    assert param in parameters, \
        "You must define {} in your parameters".format(param)

url = parameters.pop('url')
if url.startswith('env://'):
    env_var = url[6:]
    env_url = os.environ.get(env_var)
    assert env_url is not None, \
        "Missing Value - " \
        "Please set your '%s' environment variable" % env_var

    url = env_url

if 'path' not in parameters:
    parameters['path'] = PATH_PLACEHOLDER
parameters[PROP_STREAMED_FROM] = url

datapackage['resources'].append(parameters)

spew(datapackage, res_iter)
