class BaseRunner(object):

    def __init__(self, name, parameters):
        self.name = name
        self.parameters = parameters

    def get_execution_args(self, step, cwd, idx):
        raise NotImplementedError()
