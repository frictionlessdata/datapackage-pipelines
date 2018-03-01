import logging
import os
import hashlib
from importlib.util import find_spec
from importlib import import_module

from .errors import SpecError

_processor_path = None


def processor_path():
    global _processor_path
    if _processor_path is None:
        _processor_path = os.environ.get('DPP_PROCESSOR_PATH', '').split(';')
    return _processor_path


_found_files = set()


def find_file_in_path(path, remove=0):
    def finder(parts):
        global _found_files
        filename = os.path.join(*(path + parts[remove:]))
        if filename in _found_files:
            return filename
        if os.path.exists(filename):
            _found_files.add(filename)
            return filename
    return finder


def convert_dot_notation(executor):
    parts = []
    back_up = False
    while executor.startswith('..'):
        parts.append('..')
        executor = executor[1:]
        back_up = True

    if executor.startswith('.'):
        executor = executor[1:]

    executor = executor.split('.')
    executor[-1] += '.py'

    parts.extend(executor)

    return back_up, parts


_tried_imports = {}


def load_module(module):
    global _tried_imports
    if module in _tried_imports:
        return _tried_imports[module]
    module_name = 'datapackage_pipelines_'+module
    ret = None
    if find_spec(module_name):
        ret = import_module(module_name)
    _tried_imports[module] = ret
    return ret


def resolve_executor(step, path, errors):

    if 'code' in step:
        filename = hashlib.md5(step['code'].encode('utf8')).hexdigest()
        code_path = os.path.join(path, '.code')
        if not os.path.exists(code_path):
            os.mkdir(code_path)
        code_path = os.path.join(code_path, filename)
        with open(code_path, 'w') as code_file:
            code_file.write(step['code'])
        return code_path

    executor = step['run']
    back_up, parts = convert_dot_notation(executor)
    resolvers = [find_file_in_path([path])]
    if not back_up:
        if len(parts) > 1:
            module_name = parts[0]
            module = load_module(module_name)
            if module is not None:
                module = list(module.__path__)[0]
                resolvers.append(find_file_in_path([module, 'processors'], 1))

        resolvers.extend([
            find_file_in_path([path_])
            for path_ in processor_path()
        ])
        resolvers.append(find_file_in_path([os.path.dirname(__file__),
                                            '..', 'lib']))

    for resolver in resolvers:
        location = resolver(parts)
        if location is not None:
            return location

    message = "Couldn't resolve {0} at {1}".format(executor, path)
    errors.append(SpecError('Unresolved processor', message))


resolved_generators = {}


def resolve_generator(module_name):
    if module_name in resolved_generators:
        return resolved_generators[module_name]
    resolved_generators[module_name] = None
    module = load_module(module_name)
    if module is None:
        return None
    try:
        generator_class = module.Generator
    except AttributeError:
        logging.warning("Can't find 'Generator' identifier in %s", module_name)
        return None
    generator = generator_class()
    resolved_generators[module_name] = generator
    return generator
