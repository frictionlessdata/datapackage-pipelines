import os
import logging
from jsontableschema_sql import Storage
from sqlalchemy import create_engine

from datapackage_pipelines.lib.dump.dumper_base import DumperBase


class SQLDumper(DumperBase):

    def initialize(self, parameters):
        super(SQLDumper, self).initialize(parameters)
        table_to_resource = parameters['tables']
        engine = parameters.get('engine', 'env://DPP_DB_ENGINE')
        if engine.startswith('env://'):
            env_var = engine[6:]
            engine = os.environ.get(env_var)
            assert engine is not None, \
                "Couldn't connect to DB - " \
                "Please set your '%s' environment variable" % env_var
        self.engine = create_engine(engine)

        for k, v in table_to_resource.items():
            v['table-name'] = k

        self.converted_resources = \
            dict((v['resource-name'], v) for v in table_to_resource.values())

    def handle_resource(self, resource, spec, parameters, datapackage):
        resource_name = spec['name']
        if resource_name not in self.converted_resources:
            return resource
        else:
            converted_resource = self.converted_resources[resource_name]
            mode = converted_resource.get('mode', 'rewrite')
            table_name = converted_resource['table-name']
            storage = Storage(self.engine, prefix=table_name)
            if mode == 'rewrite' and '' in storage.buckets:
                storage.delete('')
            if '' not in storage.buckets:
                logging.info('Creating DB table %s', table_name)
                storage.create('', spec['schema'])
            update_keys = None
            if mode == 'update':
                update_keys = converted_resource.get('update_keys')
                if update_keys is None:
                    update_keys = spec['schema'].get('primaryKey', [])
            logging.info('Writing to DB %s -> %s (mode=%s, keys=%s)',
                         resource_name, table_name, mode, update_keys)
            return map(lambda written: written.row,
                       storage.write('', resource,
                                     keyed=True, as_generator=True,
                                     update_keys=update_keys))


SQLDumper()()
