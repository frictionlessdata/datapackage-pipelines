import os
import logging
from jsontableschema_sql import Storage
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

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
        try:
            self.engine.connect()
        except OperationalError:
            logging.exception('Failed to connect to database %s', engine)
            raise

        for k, v in table_to_resource.items():
            v['table-name'] = k

        self.converted_resources = \
            dict((v['resource-name'], v) for v in table_to_resource.values())

        self.updated_column = parameters.get("updated_column")
        self.updated_id_column = parameters.get("updated_id_column")

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
            return map(self.get_output_row,
                       storage.write('', resource,
                                     keyed=True, as_generator=True,
                                     update_keys=update_keys))

    def get_output_row(self, written):
        row, updated, updated_id = written.row, written.updated, written.updated_id
        if self.updated_column:
            row[self.updated_column] = updated
        if self.updated_id_column:
            row[self.updated_id_column] = updated_id
        return row


SQLDumper()()
