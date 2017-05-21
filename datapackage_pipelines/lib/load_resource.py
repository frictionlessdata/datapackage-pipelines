import itertools

import datapackage

from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()

url = parameters['url']
resource = parameters['resource']

selected_resource = None
datapackage = datapackage.DataPackage(url)
for i, orig_res in enumerate(datapackage.resources):
    if resource == i or resource == orig_res.descriptor.get('name'):
        dp['resources'].append(orig_res.descriptor)
        selected_resource = orig_res
        break

assert selected_resource is not None, "Failed to find resource with index or name matching %r" % resource

spew(dp, itertools.chain(res_iter, [selected_resource.iter()]))
