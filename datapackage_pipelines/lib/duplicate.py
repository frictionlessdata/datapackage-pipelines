import copy

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.kvstore import DB


def saver(resource, db):
    for idx, row in enumerate(resource):
        key = "{:08x}".format(idx)
        db.set(key, row)
        yield row


def loader(db):
    for k, value in db.items():
        yield value


def process_resources(resource_iterator, source):
    for resource in resource_iterator:
        if resource.spec['name'] == source:
            db = DB()
            yield saver(resource, db)
            yield loader(db)
        else:
            yield resource


def process_datapackage(dp, source, target_name, target_path):

    def traverse_resources(resources):
        for res in resources:
            yield res
            if res['name'] == source:
                res = copy.deepcopy(res)
                res['name'] = target_name
                res['path'] = target_path
                yield res

    dp['resources'] = list(traverse_resources(dp['resources']))
    return dp


if __name__ == '__main__':
    parameters, datapackage, resource_iterator = ingest()

    source = parameters['source']
    target_name = parameters['target-name']
    target_path = parameters['target-path']

    spew(process_datapackage(datapackage, source, target_name, target_path),
         process_resources(resource_iterator, source))
