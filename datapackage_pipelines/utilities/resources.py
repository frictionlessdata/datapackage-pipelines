def external_tabular(descriptor):
    return 'url' in descriptor


def internal_tabular(descriptor):
    return 'path' in descriptor and \
           'schema' in descriptor and \
           'url' not in descriptor


def non_tabular(descriptor):
    return 'path' in descriptor and \
           'schema' not in descriptor and \
           'url' in descriptor
