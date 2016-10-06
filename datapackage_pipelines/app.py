import logging

from .manager import pipelines
from .celery_tasks import celery_app, execute_pipeline_task


for pipeline_id, pipeline_details, pipeline_cwd, dirty \
        in pipelines():
    if dirty:
        logging.info('Executing DIRTY task %s', pipeline_id)
        execute_pipeline_task.delay(pipeline_id,
                                    pipeline_details['pipeline'],
                                    pipeline_cwd)
