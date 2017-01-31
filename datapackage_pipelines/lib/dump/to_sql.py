import os
from jsontableschema_sql import Storage
from sqlalchemy import create_engine

from datapackage_pipelines.lib.dump.dumper_base import DumperBase


class SQLDumper(DumperBase):

    def initialize(self, parameters):
        table_to_resource = parameters['tables']
        engine = parameters.get('engine', 'env://DPP_DB_ENGINE')
        if engine.startswith('env://'):
            engine = os.environ.get(engine[6:])
            assert engine is not None
        self.engine = create_engine(engine)  # pylint: disable=attribute-defined-outside-init

        for k, v in table_to_resource.items():
            v['table-name'] = k

        # pylint: disable=attribute-defined-outside-init
        self.converted_resources = \
            dict((v['resource-name'], v) for v in table_to_resource.values())

    def handle_resource(self, resource, spec, parameters, datapackage):
        resource_name = spec['name']
        if resource_name not in self.converted_resources:
            return resource
        else:
            converted_resource = self.converted_resources[resource_name]
            table_name = converted_resource['table-name']
            storage = Storage(self.engine, prefix=table_name)
            if '' in storage.buckets:
                storage.delete('')
            storage.create('', spec['schema'])
            return storage.write('', resource, keyed=True, as_generator=True)

SQLDumper()()
