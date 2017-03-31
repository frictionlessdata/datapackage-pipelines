import logging

from datapackage_pipelines.status import status
from .celery_app import celery_app
from ..specs import pipelines, PipelineSpec
from ..manager.tasks import execute_pipeline


def trigger_dirties(completed=None):
    for spec in pipelines():
        pipeline_id = spec.pipeline_id
        if (len(spec.errors) == 0 and
                (completed is None or completed in spec.dependencies) and
                (spec.dirty or status.is_waiting(pipeline_id))):
            status.register(spec.pipeline_id,
                            spec.cache_hash,
                            spec.pipeline_details,
                            spec.source_details,
                            spec.errors)
            logging.info('Executing DIRTY task %s', pipeline_id)
            pipeline_status = status.queued(pipeline_id)
            execute_pipeline_task.delay(pipeline_id,
                                        spec.pipeline_details,
                                        spec.path,
                                        'dirty-task',
                                        pipeline_status.data['queued'])


@celery_app.task
def execute_scheduled_pipeline(pipeline_id):
    for spec in pipelines():
        if spec.pipeline_id == pipeline_id:
            if len(spec.errors) == 0:
                status.register(spec.pipeline_id,
                                spec.cache_hash,
                                spec.pipeline_details,
                                spec.source_details,
                                spec.errors)
                logging.info('Executing SCHEDULED task %s', pipeline_id)
                execute_pipeline_task.delay(pipeline_id,
                                            spec.pipeline_details,
                                            spec.path,
                                            'scheduled',
                                            0)
            else:
                logging.warning('Skipping SCHEDULED task %s, as it has errors %r', pipeline_id, spec.errors)
            break


@celery_app.task
def execute_pipeline_task(pipeline_id,
                          pipeline_details,
                          pipeline_cwd,
                          trigger,
                          queue_time):

    last_queue_time = status.get_status(pipeline_id).get('queued')
    if queue_time in {0, last_queue_time}:
        spec = PipelineSpec(pipeline_id=pipeline_id,
                            pipeline_details=pipeline_details,
                            path=pipeline_cwd)
        success, _ = \
            execute_pipeline(spec,
                             trigger,
                             False)

        if success:
            trigger_dirties(pipeline_id)
