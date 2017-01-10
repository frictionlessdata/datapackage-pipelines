import logging
import time

from .manager import pipelines
from .manager.status import status

# pylint: disable=unused-import
from .celery_tasks import celery_app, execute_pipeline_task


for pipeline_id, pipeline_details, pipeline_cwd, dirty, errors \
        in pipelines():
    if dirty and len(errors) == 0:
        logging.info('Executing DIRTY task %s', pipeline_id)
        pipeline_status = status.queued(pipeline_id)
        execute_pipeline_task.delay(pipeline_id,
                                    pipeline_details['pipeline'],
                                    pipeline_cwd,
                                    'dirty-task',
                                    pipeline_status.data['queued'])
