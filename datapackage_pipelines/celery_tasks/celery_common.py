import os

from celery import Celery


REGULAR_TASK_NAME = 'datapackage_pipelines.celery_tasks.celery_tasks' + \
                        '.execute_pipeline_task'
SCHEDULED_TASK_NAME = 'datapackage_pipelines.celery_tasks.celery_tasks' + \
                        '.execute_scheduled_pipeline'
MANAGEMENT_TASK_NAME = 'datapackage_pipelines.celery_tasks.celery_tasks' + \
                        '.update_pipelines'


def get_celery_app(**kwargs):
    celery_app = Celery('dpp')

    broker = os.environ.get('DPP_CELERY_BROKER', 'redis://localhost:6379/6')

    conf = dict(
        CELERY_TIMEZONE='UTC',
        CELERY_REDIRECT_STDOUTS=False,
        BROKER_URL=broker,
        CELERY_RESULT_BACKEND=broker,
        CELERYD_LOG_LEVEL="DEBUG",
        CELERY_TASK_SERIALIZER='json',
        CELERY_RESULT_SERIALIZER='json',
        CELERY_ACCEPT_CONTENT=['json'],
        CELERYD_LOG_FORMAT='[%(asctime)s: %(levelname)s/%(processName)s(%(process)d)] %(message)s',
        CELERY_ROUTES={
            REGULAR_TASK_NAME: {'queue': 'datapackage-pipelines'},
            SCHEDULED_TASK_NAME: {'queue': 'datapackage-pipelines-management'},
            MANAGEMENT_TASK_NAME: {'queue': 'datapackage-pipelines-management'},
        }
    )
    conf.update(kwargs)

    celery_app.conf.update(**conf)

    return celery_app
