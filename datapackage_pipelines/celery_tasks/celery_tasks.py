import logging

from datapackage_pipelines.status import status
from .celery_app import celery_app
from ..specs import pipelines, PipelineSpec
from ..manager.tasks import execute_pipeline

executed_hashes = {}


@celery_app.task
def update_pipelines(action, completed_pipeline_id, completed_trigger):
    # action=init: register all pipelines, trigger anything that's dirty
    # action=update: iterate over all pipelines, register new ones, trigger dirty ones
    # action=complete: iterate over all pipelines, trigger dependencies
    # completed_pipeline_id: pipeline id that had just completed (when applicable)
    # completed_trigger: the trigger for the pipeline that had just completed (when applicable)
    logging.debug("Update Pipelines (%s)", action)
    status_all_pipeline_ids = set(sts['id'] for sts in status.all_statuses())
    executed_count = 0
    all_pipeline_ids = set()
    for spec in pipelines():
        pipeline_id = spec.pipeline_id
        all_pipeline_ids.add(pipeline_id)

        run = False
        if action == 'init':
            status.register(spec.pipeline_id,
                            spec.cache_hash,
                            spec.pipeline_details,
                            spec.source_details,
                            spec.errors)
            if spec.dirty:
                run = True
        elif action == 'update':
            registered = True
            if spec.pipeline_id not in status_all_pipeline_ids:
                registered = status.register(spec.pipeline_id,
                                             spec.cache_hash,
                                             spec.pipeline_details,
                                             spec.source_details,
                                             spec.errors)
                logging.info("NEW Pipeline: %s (registered? %s)", spec, registered)
            logging.debug('Pipeline: %s (dirty: %s, %s != %s?)',
                          spec.pipeline_id, spec.dirty, executed_hashes.get(spec.pipeline_id), spec.cache_hash)
            if (registered or spec.dirty) and executed_hashes.get(spec.pipeline_id) != spec.cache_hash:
                run = True
        elif action == 'complete':
            if completed_pipeline_id in spec.dependencies:
                if spec.dirty or completed_trigger == 'scheduled':
                    status.register(spec.pipeline_id,
                                    spec.cache_hash,
                                    spec.pipeline_details,
                                    spec.source_details,
                                    spec.errors)
                    run = True

        if len(spec.errors) == 0 and run:
            logging.info('Executing task %s (from action "%s")', pipeline_id, action)
            executed_hashes[spec.pipeline_id] = spec.cache_hash
            pipeline_status = status.queued(pipeline_id)
            execute_pipeline_task.delay(pipeline_id,
                                        spec.pipeline_details,
                                        spec.path,
                                        'dirty-task' if completed_trigger is None else completed_trigger,
                                        pipeline_status.data['queued'])
            executed_count += 1
            if executed_count == 4 and action == 'update':
                # Limit ops on update only
                break

    if executed_count == 0:
        extra_pipelines = status_all_pipeline_ids.difference(all_pipeline_ids)
        for pipeline_id in extra_pipelines:
            logging.info("Removing Pipeline: %s", pipeline_id)
            status.deregister(pipeline_id)


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

    pipeline_status = status.get_status(pipeline_id)
    last_queue_time = None
    if pipeline_status is not None:
        last_queue_time = pipeline_status.get('queued')
    if queue_time in {0, last_queue_time} or last_queue_time is None:
        spec = PipelineSpec(pipeline_id=pipeline_id,
                            pipeline_details=pipeline_details,
                            path=pipeline_cwd)
        success, _, _ = \
            execute_pipeline(spec,
                             trigger,
                             False)

        if success:
            update_pipelines.delay('complete', pipeline_id, trigger)
            update_pipelines.delay('update', None, None)
