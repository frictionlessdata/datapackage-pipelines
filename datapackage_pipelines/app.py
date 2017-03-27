# pylama:ignore=W0611
from .celery_tasks import celery_app, trigger_dirties


trigger_dirties()
