from celery import Celery

from datapackage_pipelines.manager import initialize_pipeline


CELERY_SCHEDULE = initialize_pipeline()

CELERY = Celery()
CELERY.conf.update(CELERYBEAT_SCHEDULE=CELERY_SCHEDULE,
                   CELERY_TIMEZONE='UTC',
                   CELERY_REDIRECT_STDOUTS=False)
