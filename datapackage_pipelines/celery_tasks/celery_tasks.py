import logging

from .celery_app import celery_app

from ..manager import pipelines
from ..manager.tasks import execute_pipeline
from ..manager.status import status


def trigger_dirties(run_all=False):
    for pipeline_id, pipeline_details, pipeline_cwd, dirty, errors \
            in pipelines():
        if dirty and len(errors) == 0 and \
                (run_all or status.is_waiting(pipeline_id)):
            logging.info('Executing DIRTY task %s', pipeline_id)
            pipeline_status = status.queued(pipeline_id)
            execute_pipeline_task.delay(pipeline_id,
                                        pipeline_details['pipeline'],
                                        pipeline_cwd,
                                        'dirty-task',
                                        pipeline_status.data['queued'])

@celery_app.task
def execute_pipeline_task(pipeline_id,
                          pipeline_steps,
                          pipeline_cwd,
                          trigger,
                          queue_time):

    last_queue_time = status.get_status(pipeline_id).get('queued')
    if queue_time in {0, last_queue_time}:
        success, _ = \
            execute_pipeline(pipeline_id,
                             pipeline_steps,
                             pipeline_cwd,
                             trigger,
                             False)

#        if success:
#            trigger_dirties()
