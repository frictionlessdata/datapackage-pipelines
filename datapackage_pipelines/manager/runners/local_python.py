import os
import sys
import shlex

from ...utilities.extended_json import json
from .base_runner import BaseRunner


class LocalPythonRunner(BaseRunner):

    def get_execution_args(self, step, _, idx):
        return [
            sys.executable,
            step['executor'],
            str(idx),
            json.dumps(step.get('parameters', {})),
            str(step.get('validate', False)),
            step.get('_cache_hash') if step.get('cache') else ''
        ]


class WrappedPythonRunner(LocalPythonRunner):

    def get_execution_args(self, step, cwd, idx):
        args = super(WrappedPythonRunner, self).get_execution_args(step, cwd, idx)
        for i in range(len(args)):
            args[i] = '\\\"' + args[i].replace('"', '\\\\\\\"') + '\\\"'
        cmd = " ".join(args)
        abspath = os.path.abspath(cwd)
        cmd = self.parameters['wrapper'].format(path=cwd,
                                                abspath=abspath,
                                                cmd=cmd,
                                                env=os.environ)
        args = shlex.split(cmd)
        return args
