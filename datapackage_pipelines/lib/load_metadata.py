import datapackage

from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()

url = parameters['url']

datapackage = datapackage.DataPackage(url)
for k, v in datapackage.descriptor.items():
    if k != 'resources':
        dp[k] = v

spew(dp, res_iter)
