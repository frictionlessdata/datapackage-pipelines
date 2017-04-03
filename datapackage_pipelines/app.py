# pylama:ignore=W0611
from .celery_tasks import celery_app, trigger_dirties
from .manager.logging_config import logging

trigger_dirties()
