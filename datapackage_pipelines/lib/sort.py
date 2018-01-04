from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.kvstore import KVStore
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher


class KeyCalc(object):

    def __init__(self, key_spec):
        self.key_spec = key_spec

    def __call__(self, row):
        return self.key_spec.format(**row)


parameters, datapackage, resource_iterator = ingest()

resources = ResourceMatcher(parameters['resources'])
key_calc = KeyCalc(parameters['sort-by'])


def sorter(resource):
    db = KVStore()
    for row_num, row in enumerate(resource):
        key = key_calc(row) + "{:08x}".format(row_num)
        db[key] = row
    for key in db.keys():
        yield db[key]


def new_resource_iterator(resource_iterator_):
    for resource in resource_iterator_:
        if resources.match(resource.spec['name']):
            yield sorter(resource)
        else:
            yield resource


spew(datapackage, new_resource_iterator(resource_iterator))
