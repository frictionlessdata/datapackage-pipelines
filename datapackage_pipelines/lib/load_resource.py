import itertools

import datapackage

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher
from datapackage_pipelines.utilities.resources import tabular, PROP_STREAMING


class ResourceLoader(object):

    def __init__(self):
        self.parameters, self.dp, self.res_iter = ingest()

    def __call__(self):
        url = self.parameters['url']
        resource = self.parameters['resource']
        stream = self.parameters.get('stream', True)
        name_matcher = ResourceMatcher(resource) if isinstance(resource, str) else None
        resource_index = resource if isinstance(resource, int) else None

        selected_resources = []
        found = False
        dp = datapackage.DataPackage(url)
        for i, orig_res in enumerate(dp.resources):
            if resource_index == i or \
                    (name_matcher is not None and name_matcher.match(orig_res.descriptor.get('name'))):
                found = True
                self.dp['resources'].append(orig_res.descriptor)
                if tabular(orig_res.descriptor) and stream:
                    orig_res.descriptor[PROP_STREAMING] = True
                    selected_resources.append(orig_res.iter(keyed=True))
                else:
                    orig_res.descriptor[PROP_STREAMING] = False

        assert found, "Failed to find resource with index or name matching %r" % resource
        spew(self.dp, itertools.chain(self.res_iter, selected_resources))


if __name__ == '__main__':
    ResourceLoader()()
