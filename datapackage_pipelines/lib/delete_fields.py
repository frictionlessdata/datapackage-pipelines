from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher

parameters, datapackage, resource_iterator = ingest()

resources = ResourceMatcher(parameters.get('resources'))
fields = parameters.get('fields', [])


def delete_fields(datapackage_):
    dp_resources = datapackage_.get('resources', [])
    for resource_ in dp_resources:
        if resources.match(resource_['name']):
            dp_fields = resource_['schema'].get('fields', [])
            field_names = [f['name'] for f in dp_fields]
            non_existings = [f for f in fields if f not in field_names]

            assert len(non_existings) == 0, \
                "Can't find following field(s): %s" % '; '.join(non_existings)

            new_fields = list(
                filter(lambda x: x['name'] not in fields, dp_fields))
            resource_['schema']['fields'] = new_fields
    return datapackage_


def process_resource(rows):
    for row in rows:
        for field in fields:
            del row[field]
        yield row


def process_resources(resource_iterator_):
    for resource in resource_iterator_:
        spec = resource.spec
        if not resources.match(spec['name']):
            yield resource
        else:
            yield process_resource(resource)


spew(delete_fields(datapackage), process_resources(resource_iterator))
