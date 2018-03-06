import os

from celery.schedules import crontab

from .celery_common import get_celery_app, MANAGEMENT_TASK_NAME, SCHEDULED_TASK_NAME
from .celery_tasks import build_dependents
from datapackage_pipelines.specs import pipelines
from datapackage_pipelines.status import status_mgr

import logging

kw = {}
if os.environ.get('SCHEDULER'):
    CELERY_SCHEDULE = {
        '/management': {
            'task': MANAGEMENT_TASK_NAME,
            'schedule': crontab(),
            'args': ('update', None, None),
            'options': {'queue': 'datapackage-pipelines-management'}
        }
    }

    for spec in pipelines():
        if spec.schedule is not None:
            entry = {
                'task': SCHEDULED_TASK_NAME,
                'schedule': crontab(*spec.schedule),
                'args': (spec.pipeline_id,),
                'options': {'queue': 'datapackage-pipelines-management'}
            }
            CELERY_SCHEDULE[spec.pipeline_id] = entry
            logging.info('SCHEDULING task %r: %r', spec.pipeline_id, spec.schedule)

        ps = status_mgr().get(spec.pipeline_id)
        ex = ps.get_last_execution()
        if ex is not None and not ex.finish_time:
            ex.invalidate()
            ex.finish_execution(False, {}, ['Cancelled'])

    kw = dict(CELERYBEAT_SCHEDULE=CELERY_SCHEDULE)

logging.error('CELERY INITIALIZING')
celery_app = get_celery_app(**kw)

if os.environ.get('SCHEDULER'):
    build_dependents()
    celery_app.send_task(MANAGEMENT_TASK_NAME, ('init', None, None))
