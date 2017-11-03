from celery.schedules import crontab

from .celery_common import get_celery_app, MANAGEMENT_TASK_NAME, SCHEDULED_TASK_NAME
from .celery_tasks import build_dependents
from datapackage_pipelines.specs import pipelines

import logging


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


logging.error('CELERY INITIALIZING')
celery_app = get_celery_app(CELERYBEAT_SCHEDULE=CELERY_SCHEDULE)
build_dependents()
celery_app.send_task(MANAGEMENT_TASK_NAME, ('init', None, None))
