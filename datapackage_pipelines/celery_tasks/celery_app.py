import os

from celery import Celery
from celery.schedules import crontab

from datapackage_pipelines.manager import pipelines

TASK_NAME = 'datapackage_pipelines.celery_tasks.celery_tasks.execute_pipeline_task'


CELERY_SCHEDULE = {}

for pipeline_id, pipeline_details, pipeline_cwd, dirty \
        in pipelines():
    entry = {
        'task': TASK_NAME,
        'schedule': crontab(*pipeline_details['schedule']),
        'args': (pipeline_id,
                 pipeline_details['pipeline'],
                 pipeline_cwd)
    }
    CELERY_SCHEDULE[pipeline_id] = entry

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
