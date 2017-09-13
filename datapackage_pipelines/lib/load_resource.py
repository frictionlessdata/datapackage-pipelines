import itertools

import datapackage

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher
from datapackage_pipelines.utilities.resources import tabular, PROP_STREAMING

parameters, dp, res_iter = ingest()

url = parameters['url']
resource = parameters['resource']
name_matcher = ResourceMatcher(resource) if isinstance(resource, str) else None
resource_index = resource if isinstance(resource, int) else None

selected_resources = []
found = False
datapackage = datapackage.DataPackage(url)
for i, orig_res in enumerate(datapackage.resources):
    if resource_index == i or \
          (name_matcher is not None and name_matcher.match(orig_res.descriptor.get('name'))):
        found = True
        dp['resources'].append(orig_res.descriptor)
        if tabular(orig_res.descriptor):
            orig_res.descriptor[PROP_STREAMING] = True
            selected_resources.append(orig_res.iter(keyed=True))

assert found, "Failed to find resource with index or name matching %r" % resource

spew(dp, itertools.chain(res_iter, selected_resources))
