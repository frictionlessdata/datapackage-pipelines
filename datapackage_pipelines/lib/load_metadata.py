import datapackage

from datapackage_pipelines.wrapper import ingest, spew, get_dependency_datapackage_url

dep_prefix = 'dependency://'

parameters, dp, res_iter = ingest()

url = parameters['url']
if url.startswith(dep_prefix):
    dependency = url[len(dep_prefix):].strip()
    url = get_dependency_datapackage_url(dependency)
    assert url is not None, "Failed to fetch output datapackage for dependency '%s'" % dependency

datapackage = datapackage.DataPackage(url)
for k, v in datapackage.descriptor.items():
    if k != 'resources':
        dp[k] = v

spew(dp, res_iter)
