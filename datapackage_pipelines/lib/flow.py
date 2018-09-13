import logging
import sys
from contextlib import redirect_stderr, redirect_stdout
from importlib import import_module
from datapackage_pipelines.wrapper import ingest, spew


class StdoutWriter:

    def write(self, message):
        message = message.strip()
        if message:
            logging.info(message)

    def flush(self):
        pass


class StderrWriter:

    def write(self, message):
        message = message.strip()
        if (message):
            logging.error(message)

    def flush(self):
        pass


parameters, datapackage, resources, stats = ingest() + ({},)

with redirect_stderr(StderrWriter()):
    with redirect_stdout(StdoutWriter()):
        sys.path.append(parameters.pop('__path'))
        import_module(parameters.pop('__flow')).flow(parameters, stats).process()

spew(datapackage, resources, stats)
