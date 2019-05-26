from dataflows import Flow, set_type, validate, delete_fields
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow


def flow(parameters):
    resources = parameters.get('resources')
    regex = parameters.get('regex', True)
    if 'types' in parameters:
        return Flow(
            *[
                set_type(name, resources=resources, regex=regex, **options)
                if options is not None else
                delete_fields([name], resources=resources)
                for name, options in parameters['types'].items()
            ]
        )
    else:
        return Flow(
            validate()
        )


if __name__ == '__main__':
    with ingest() as ctx:
        print(flow(ctx.parameters).chain)
        spew_flow(flow(ctx.parameters), ctx)
