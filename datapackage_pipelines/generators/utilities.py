def arg_to_step(arg):
    if isinstance(arg, str):
        return {'run': arg}
    else:
        return dict(zip(['run', 'parameters', 'cache'], arg))


def steps(*args):
    return [arg_to_step(arg) for arg in args]
