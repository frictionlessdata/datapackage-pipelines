import os
import yaml

from .local_python import LocalPythonRunner, WrappedPythonRunner


class RunnerConfiguration(object):

    ENV_VAR = 'DPP_RUNNER_CONFIG'
    DEFAULT_RUNNER_CONFIG = 'dpp-runners.yaml'

    def __init__(self):

        config_fn = os.environ.get(self.ENV_VAR, self.DEFAULT_RUNNER_CONFIG)
        if os.path.exists(config_fn):
            self.config = yaml.load(open(config_fn))
        else:
            self.config = {}

    def get_runner_class(self, kind):
        return {
            'local-python': LocalPythonRunner,
            'wrapped-python': WrappedPythonRunner,
        }.get(kind, LocalPythonRunner)

    def get_runner(self, name):
        runner_config = self.config.get(name, {})
        kind = runner_config.get('kind')
        parameters = runner_config.get('parameters')
        return self.get_runner_class(kind)(name, parameters)
