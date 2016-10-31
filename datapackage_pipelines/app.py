import logging

from .manager import pipelines

# pylint: disable=unused-import
from .celery_tasks import celery_app, execute_pipeline_task


for pipeline_id, pipeline_details, pipeline_cwd, dirty, errors \
        in pipelines():
    if dirty and len(errors) == 0:
        logging.info('Executing DIRTY task %s', pipeline_id)
        execute_pipeline_task.delay(pipeline_id,
                                    pipeline_details['pipeline'],
                                    pipeline_cwd,
                                    'dirty-task')
