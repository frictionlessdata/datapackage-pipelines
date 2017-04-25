import os

from celery import Celery
from celery.schedules import crontab

from datapackage_pipelines.specs import pipelines

import logging

REGULAR_TASK_NAME = 'datapackage_pipelines.celery_tasks.celery_tasks' + \
                        '.execute_pipeline_task'
SCHEDULED_TASK_NAME = 'datapackage_pipelines.celery_tasks.celery_tasks' + \
                        '.execute_scheduled_pipeline'
MANAGEMENT_TASK_NAME = 'datapackage_pipelines.celery_tasks.celery_tasks' + \
                        '.update_pipelines'

CELERY_SCHEDULE = {
    '/management': {
        'task': MANAGEMENT_TASK_NAME,
        'schedule': crontab(),
        'args': ('update', None, None),
        'options': {'queue': 'datapackage-pipelines-management'}
    }
}

# register_all_pipelines()

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

celery_app = Celery('dpp')
celery_app.conf.update(CELERYBEAT_SCHEDULE=CELERY_SCHEDULE,
                       CELERY_TIMEZONE='UTC',
                       CELERY_REDIRECT_STDOUTS=False,
                       BROKER_URL=os.environ.get('CELERY_BROKER', 'amqp://'),
                       CELERY_RESULT_BACKEND=os.environ.get('CELERY_BACKEND',
                                                            'amqp://'),
                       CELERYD_LOG_LEVEL="INFO",
                       CELERY_TASK_SERIALIZER='json',
                       CELERY_RESULT_SERIALIZER='json',
                       CELERY_ACCEPT_CONTENT=['json'],
                       CELERY_ROUTES={
                            REGULAR_TASK_NAME: {'queue': 'datapackage-pipelines'},
                            SCHEDULED_TASK_NAME: {'queue': 'datapackage-pipelines-management'},
                            MANAGEMENT_TASK_NAME: {'queue': 'datapackage-pipelines-management'},
                       })
