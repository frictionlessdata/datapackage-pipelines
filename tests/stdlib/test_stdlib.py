import os, logging
from datapackage_pipelines.utilities.lib_test_helpers import ProcessorFixtureTestsBase
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text

ROOT_PATH = os.path.join(os.path.dirname(__file__), '..', '..')
ENV = os.environ.copy()
ENV['PYTHONPATH'] = ROOT_PATH

ENV['EXISTENT_ENV'] = 'tests/data/sample.csv'

DEFAULT_TEST_DB = "sqlite://"
ENV['DPP_DB_ENGINE'] = os.environ.get("OVERRIDE_TEST_DB", DEFAULT_TEST_DB)


class StdlibfixtureTests(ProcessorFixtureTestsBase):

    def _get_procesor_env(self, filename):
        if ENV['DPP_DB_ENGINE'] != DEFAULT_TEST_DB:
            engine = create_engine(ENV['DPP_DB_ENGINE'])
            engine.execute(text("DROP TABLE IF EXISTS test;"))
        if filename == "dump_to_sql_update_mode__update":
            engine = create_engine(ENV['DPP_DB_ENGINE'])
            engine.execute(text("""
                CREATE TABLE test (
                  id integer not null primary key,
                  mystring text,
                  mynumber double precision,
                  mydate date
                )
            """))
            engine.execute(text("""
                INSERT INTO test VALUES (1, 'foo', 5.6, null);
            """))
        return ENV

    def _get_processor_file(self, processor):
        processor = processor.replace('.', '/')
        return os.path.join(ROOT_PATH, 'datapackage_pipelines', 'lib', processor.strip() + '.py')


for filename, _func in StdlibfixtureTests(os.path.join(os.path.dirname(__file__), 'fixtures')).get_tests():
    globals()['test_stdlib_%s' % filename] = _func

