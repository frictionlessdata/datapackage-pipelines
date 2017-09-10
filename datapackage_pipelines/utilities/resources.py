def is_a_url(path):
    return (path is not None and isinstance(path, str) and
            (path.startswith('http://') or
             path.startswith('https://'))
            )


def streamable(descriptor):
    return PROP_STREAMED_FROM in descriptor


def internal_tabular(descriptor):
    return 'path' in descriptor and \
           not is_a_url(get_path(descriptor)) and \
           get_path(descriptor) != PATH_PLACEHOLDER and \
           'schema' in descriptor


def non_tabular(descriptor):
    return 'path' in descriptor and \
           not is_a_url(get_path(descriptor)) and \
           'schema' not in descriptor and \
           streamable(descriptor)


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


PATH_PLACEHOLDER = '.'
PROP_STREAMED_FROM = 'streamedFrom'
