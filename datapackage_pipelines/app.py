import os

from celery import Celery

from datapackage_pipelines.manager import initialize_pipeline, TASK_NAME


CELERY_SCHEDULE = initialize_pipeline()

celery = Celery('dpp')
celery.conf.update(CELERYBEAT_SCHEDULE=CELERY_SCHEDULE,
                   CELERY_TIMEZONE='UTC',
                   CELERY_REDIRECT_STDOUTS=False,
                   BROKER_URL=os.environ.get('CELERY_BROKER', 'amqp://'),
                   CELERY_RESULT_BACKEND=os.environ.get('CELERY_BACKEND', 'amqp://'),
                   CELERYD_LOG_LEVEL="INFO",
                   CELERY_TASK_SERIALIZER='json',
                   CELERY_ACCEPT_CONTENT=['json'],
                   CELERY_RESULT_SERIALIZER='json',
                   CELERY_ROUTES={
                       TASK_NAME: {'queue': 'datapackage-pipelines'},
                   })

