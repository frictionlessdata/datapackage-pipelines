import os

from celery import Celery
from celery.schedules import crontab

from datapackage_pipelines.specs import pipelines, register_all_pipelines

TASK_NAME = 'datapackage_pipelines.celery_tasks.celery_tasks' + \
                '.execute_pipeline_task'

CELERY_SCHEDULE = {}

register_all_pipelines()

for spec in pipelines():
    if len(spec.errors) == 0 and spec.schedule is not None:
        entry = {
            'task': TASK_NAME,
            'schedule': crontab(*spec.schedule),
            'args': (spec.pipeline_id,
                     spec.pipeline_details['pipeline'],
                     spec.path,
                     'schedule',
                     0)
        }
        CELERY_SCHEDULE[spec.pipeline_id] = entry

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
                           TASK_NAME: {'queue': 'datapackage-pipelines'},
                       })
