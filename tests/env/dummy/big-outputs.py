import logging
import itertools
import os

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING

params, dp, res_iter = ingest()

big_string = 'z'*64*1024

logging.info('Look at me %s', big_string)

dp['name'] = 'a'
dp['resources'].append({
    'name': 'aa%f' % os.getpid(),
    'path': 'data/bla.csv',
    'schema': {
        'fields': [
            {'name': 'a', 'type': 'string'}
        ]
    },
    'very-large-prop': '*' * 100 * 1024,
    PROP_STREAMING: True
})

res = iter([{'a': big_string}])

spew(dp, itertools.chain(res_iter, [res]))
