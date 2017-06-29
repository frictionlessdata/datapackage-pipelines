import itertools

import datapackage

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher

parameters, dp, res_iter = ingest()

url = parameters['url']
resource = parameters['resource']
name_matcher = ResourceMatcher(resource) if isinstance(resource, str) else None
resource_index = resource if isinstance(resource, int) else None

selected_resources = []
datapackage = datapackage.DataPackage(url)
for i, orig_res in enumerate(datapackage.resources):
    if resource_index == i or \
          (name_matcher is not None and name_matcher.match(orig_res.descriptor.get('name'))):
        dp['resources'].append(orig_res.descriptor)
        selected_resources.append(orig_res.iter())

assert len(selected_resources) > 0, "Failed to find resource with index or name matching %r" % resource

spew(dp, itertools.chain(res_iter, selected_resources))
