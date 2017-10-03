import logging
import os

from ..status import status

from .celery_app import celery_app
from ..specs import pipelines, PipelineSpec, register_all_pipelines
from ..manager.tasks import execute_pipeline

executed_hashes = {}
dependencies = {}
dependents = {}
already_init = False


def collect_dependencies(pipeline_ids):
    if pipeline_ids is None:
        return None
    ret = set()
    for pipeline_id in pipeline_ids:
        deps = dependencies.get(pipeline_id)
        if deps is not None:
            for dep in deps:
                ret.update(collect_dependencies(dep))
            ret.update(deps)
    return ret


@celery_app.task
def update_pipelines(action, completed_pipeline_id, completed_trigger):
    # action=init: register all pipelines, trigger anything that's dirty
    # action=update: iterate over all pipelines, register new ones, trigger dirty ones
    # action=complete: iterate over all pipelines, trigger dependencies
    # completed_pipeline_id: pipeline id that had just completed (when applicable)
    # completed_trigger: the trigger for the pipeline that had just completed (when applicable)
    global already_init
    if action == 'init':
        if already_init:
            return
        else:
            register_all_pipelines()
    already_init = True

    logging.debug("Update Pipelines (%s)", action)
    status_all_pipeline_ids = set(sts['id'] for sts in status.all_statuses())
    executed_count = 0
    all_pipeline_ids = set()

    if action == 'complete':
        filter = collect_dependencies(dependents.get(completed_pipeline_id))
        logging.info("DEPENDENTS Pipeline: %s <- %s", completed_pipeline_id, filter)
    else:
        filter = ('',)

    for spec in pipelines(filter):
        pipeline_id = spec.pipeline_id
        all_pipeline_ids.add(pipeline_id)

        run = False
        if action == 'init':
            status.register(spec.pipeline_id,
                            spec.cache_hash,
                            spec.pipeline_details,
                            spec.source_details,
                            spec.errors)
            for dep in spec.dependencies:
                dependents.setdefault(dep, set()).add(spec.pipeline_id)
            dependencies[spec.pipeline_id] = spec.dependencies
            if spec.dirty:
                run = True
        elif action == 'update':
            registered = False
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
                    logging.info("DEPENDENT Pipeline: %s (%d errors) (from ...%s)",
                                 spec.pipeline_id, len(spec.errors), os.path.basename(completed_pipeline_id))
                    run = True

        if len(spec.errors) == 0 and run:
            state = status.get_status(pipeline_id)['state']
            logging.info('EXECUTING task %s @ %s (from action "%s")', pipeline_id, state, action)
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

    if executed_count == 0 and action != 'complete':
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
