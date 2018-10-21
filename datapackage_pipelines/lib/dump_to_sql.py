from dataflows import Flow, dump_to_sql
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow


def flow(parameters):
    return Flow(
        dump_to_sql(
            parameters['tables'],
            engine=parameters.get('engine', 'env://DPP_DB_ENGINE'),
            updated_column=parameters.get("updated_column"),
            updated_id_column=parameters.get("updated_id_column")
        )
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
