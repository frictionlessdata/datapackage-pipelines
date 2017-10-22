import os


def is_a_url(path):
    return (path is not None and isinstance(path, str) and
            (path.startswith('http://') or
             path.startswith('https://'))
            )


def tabular(descriptor):
    return 'schema' in descriptor


def streaming(descriptor):
    return descriptor.get(PROP_STREAMING)


def streamable(descriptor):
    return PROP_STREAMED_FROM in descriptor and \
           not streaming(descriptor)


def get_path(descriptor):
    path = descriptor.get('path')
    if isinstance(path, str):
        return path
    if isinstance(path, list):
        if len(path) > 0:
            return path.pop(0)
        else:
            return None
    assert path is None, '%r' % path
    return None


def insert_hash_in_path(descriptor, hash):
    path = descriptor.get('path')
    if isinstance(path, str):
        path_list = path.split('/')
        hashed = os.path.join(hash, path_list.pop(-1))
        path_list.append(hashed)
        descriptor['path'] = '/'.join(path_list)
    if isinstance(path, list):
        if len(path) > 0:
            path = path[0]
            path_list = path.split('/')
            hashed = os.path.join(hash, path_list.pop(-1))
            path_list.append(hashed)
            descriptor['path'] = ['/'.join(path_list)]


PATH_PLACEHOLDER = '.'
PROP_STREAMED_FROM = 'dpp:streamedFrom'
PROP_STREAMING = 'dpp:streaming'
