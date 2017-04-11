import os
from datapackage_pipelines.utilities.lib_test_helpers import ProcessorFixtureTestsBase

ROOT_PATH = os.path.join(os.path.dirname(__file__), '..', '..')
ENV = os.environ.copy()
ENV['PYTHONPATH'] = ROOT_PATH


class StdlibfixtureTests(ProcessorFixtureTestsBase):

    def _get_procesor_env(self):
        return ENV

    def _get_processor_file(self, processor):
        processor = processor.replace('.', '/')
        return os.path.join(ROOT_PATH, 'datapackage_pipelines', 'lib', processor.strip() + '.py')


for filename, testfunc in StdlibfixtureTests(os.path.join(os.path.dirname(__file__), 'fixtures')).get_tests():
    globals()['test_stdlib_%s' % filename] = testfunc

