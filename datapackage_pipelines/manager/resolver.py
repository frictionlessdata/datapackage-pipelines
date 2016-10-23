import logging
import os

PROCESSOR_PATH = os.environ.get('DATAPIPELINES_PROCESSOR_PATH', '').split(';')


def find_file_in_path(path, remove=0):
    def finder(parts):
        filename = os.path.join(*(path + parts[remove:]))
        if os.path.exists(filename):
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


def load_module(module):
    module_name = 'datapackage_pipelines_'+module
    try:
        module = __import__(module_name)
        return module
    except ImportError:
        logging.warning("Couldn't import %s", module_name)


def resolve_executor(executor, path):

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
            find_file_in_path([path])
            for path in PROCESSOR_PATH
        ])
        resolvers.append(find_file_in_path([os.path.dirname(__file__),
                                            '..', 'lib']))

    for resolver in resolvers:
        location = resolver(parts)
        if location is not None:
            return location

    raise FileNotFoundError("Couldn't resolve {0} at {1}"
                            .format(executor, path))

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
