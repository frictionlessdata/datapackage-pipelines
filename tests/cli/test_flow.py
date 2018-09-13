from dataflows import Flow, dump_to_path, PackageWrapper


def hello_dataflows(package: PackageWrapper):
    print('hello dataflows')
    yield package.pkg
    yield from package


def flow(parameters):
    return Flow(hello_dataflows,
                [{parameters['attr']: 'bar'},
                 {parameters['attr']: 'baz'}],
                dump_to_path('test_flow_data'))
