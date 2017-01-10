from .celery_app import celery_app

from ..manager.tasks import execute_pipeline
from ..manager.status import status

@celery_app.task
def execute_pipeline_task(pipeline_id, pipeline_steps, pipeline_cwd, trigger, queue_time):
    last_queue_time = status.get_status(pipeline_id).get('queued')
    if queue_time in {0, last_queue_time}:
        execute_pipeline(pipeline_id,
                         pipeline_steps,
                         pipeline_cwd,
                         trigger,
                         False)
