from dataflows import Flow, dump_to_path, PackageWrapper, load, update_package


def hello_dataflows(package: PackageWrapper):
    print('hello dataflows')
    yield package.pkg
    yield from package


def flow(parameters, datapackage, resources, stats):
    stats['foo_values'] = 0

    def add_foo_field(package: PackageWrapper):
        package.pkg.descriptor['resources'][0]['schema']['fields'] += [
            {'name': parameters['attr'], 'type': 'string'}]
        yield package.pkg
        yield from package

    def add_foo_value(row):
        row[parameters['attr']] = 'foo'
        stats['foo_values'] += 1

    return Flow(update_package(name='_'),
                hello_dataflows,
                add_foo_field,
                add_foo_value)
