import itertools

import datapackage

from datapackage_pipelines.wrapper import ingest, spew, get_dependency_datapackage_url
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher
from datapackage_pipelines.utilities.resources import tabular, PROP_STREAMING, \
    PROP_STREAMED_FROM


class ResourceLoader(object):

    def __init__(self):
        self.parameters, self.dp, self.res_iter = ingest()

    def __call__(self):
        url = self.parameters['url']
        limit_rows = self.parameters.get('limit-rows')
        dep_prefix = 'dependency://'
        if url.startswith(dep_prefix):
            dependency = url[len(dep_prefix):].strip()
            url = get_dependency_datapackage_url(dependency)
            assert url is not None, "Failed to fetch output datapackage for dependency '%s'" % dependency
        resource = self.parameters['resource']
        stream = self.parameters.get('stream', True)
        name_matcher = ResourceMatcher(resource) if isinstance(resource, str) else None
        resource_index = resource if isinstance(resource, int) else None

        selected_resources = []
        found = False
        dp = datapackage.DataPackage(url)
        dp = self.process_datapackage(dp)
        for i, orig_res in enumerate(dp.resources):
            if resource_index == i or \
                    (name_matcher is not None and name_matcher.match(orig_res.descriptor.get('name'))):
                found = True
                orig_res.descriptor[PROP_STREAMED_FROM] = orig_res.source
                self.dp['resources'].append(orig_res.descriptor)
                if tabular(orig_res.descriptor) and stream:
                    orig_res.descriptor[PROP_STREAMING] = True
                    orig_res_iter = orig_res.iter(keyed=True)
                    if limit_rows:
                        orig_res_iter = itertools.islice(orig_res_iter, limit_rows)
                    selected_resources.append(orig_res_iter)
                else:
                    orig_res.descriptor[PROP_STREAMING] = False

        assert found, "Failed to find resource with index or name matching %r" % resource
        spew(self.dp, itertools.chain(self.res_iter, selected_resources))

    def process_datapackage(self, dp_):
        return dp_


if __name__ == '__main__':
    ResourceLoader()()
