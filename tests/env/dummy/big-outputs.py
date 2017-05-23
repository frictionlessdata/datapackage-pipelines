import logging
import itertools
import os

from datapackage_pipelines.wrapper import ingest, spew

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
    }
})

res = iter([{'a': big_string}])

spew(dp, itertools.chain(res_iter, [res]))
