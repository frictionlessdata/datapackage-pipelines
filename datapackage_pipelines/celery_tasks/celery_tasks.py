import logging
import os

from ..utilities.execution_id import gen_execution_id

from ..status import status_mgr
from ..status.pipeline_status import PipelineStatus

from .dependency_manager import DependencyManager
from .celery_common import get_celery_app
from ..specs import pipelines, PipelineSpec
from ..manager.tasks import execute_pipeline


celery_app = get_celery_app()


executed_hashes = {}
# dependencies = {}
# dependents = {}
already_init = False
dep_mgr = None


def get_dep_mgr() -> DependencyManager:
    global dep_mgr
    if dep_mgr is None:
        dep_mgr = DependencyManager()
    return dep_mgr


def build_dependents():
    dm = get_dep_mgr()
    for spec in pipelines():  # type: PipelineSpec
        dm.update(spec)


def collect_dependencies(pipeline_ids):
    if pipeline_ids is None:
        return None
    dm = get_dep_mgr()
    ret = set()
    for pipeline_id in pipeline_ids:
        ret.add(pipeline_id)
        deps = dm.get_dependencies(pipeline_id)
        if deps is not None:
            ret.update(collect_dependencies(deps))
    return ret


def queue_pipeline(ps: PipelineStatus, spec: PipelineSpec, trigger):
    eid = gen_execution_id()
    logging.info('%s QUEUEING %s task %s', eid[:8], trigger.upper(), spec.pipeline_id)
    if ps.queue_execution(eid, trigger):
        execute_pipeline_task.delay(spec.pipeline_id,
                                    spec.pipeline_details,
                                    spec.path,
                                    trigger,
                                    eid)
        return True
    return False


def execute_update_pipelines():
    update_pipelines.delay('update', None, None)


@celery_app.task
def update_pipelines(action, completed_pipeline_id, completed_trigger):
    # action=scheduled: start a scheduled pipeline
    # action=update: iterate over all pipelines, register new ones, trigger dirty ones
    # action=complete: iterate over all pipelines, trigger dependencies
    # completed_pipeline_id: pipeline id that had just completed (or scheduled pipeline)
    # completed_trigger: the trigger for the pipeline that had just completed (when applicable)

    logging.info("Update Pipelines (%s)", action)
    status_all_pipeline_ids = set(status_mgr().all_pipeline_ids())
    executed_count = 0
    all_pipeline_ids = set()
    dm = get_dep_mgr()

    if action == 'complete':
        filter = collect_dependencies(dm.get_dependents(completed_pipeline_id))
        logging.info("DEPENDENTS Pipeline: %s <- %s", completed_pipeline_id, filter)
    else:
        filter = ('',)

    for spec in pipelines(filter):  # type: PipelineSpec
        all_pipeline_ids.add(spec.pipeline_id)
        ps = status_mgr().get(spec.pipeline_id)
        ps.init(spec.pipeline_details,
                spec.source_details,
                spec.validation_errors,
                spec.cache_hash)

        if action == 'init':
            psle = ps.get_last_execution()
            if psle is not None and not psle.finish_time:
                psle.invalidate()
                psle.finish_execution(False, {}, ['Cancelled'])

        elif action == 'update':
            if spec.pipeline_id not in status_all_pipeline_ids:
                dm.update(spec)
                logging.info("NEW Pipeline: %s", spec)
                ps.save()
            logging.debug('Pipeline: %s (dirty: %s, #ex=%s, ch=%s ex0-cache=%s)',
                          spec.pipeline_id, ps.dirty(), len(ps.executions), ps.cache_hash,
                          ps.executions[0].cache_hash if len(ps.executions) > 0 else None)

        elif action == 'complete':
            if completed_pipeline_id in spec.dependencies:
                logging.info("DEPENDENT Pipeline: %s (%d errors) (from ...%s), trigger=%s",
                             spec.pipeline_id, len(spec.validation_errors),
                             os.path.basename(completed_pipeline_id),
                             completed_trigger)
                ps.save()
            else:
                continue

        elif action == 'scheduled':
            if completed_pipeline_id != spec.pipeline_id:
                continue

        last_successful = ps.state() == 'SUCCEEDED'
        if ps.runnable() and \
                (ps.dirty() or
                 completed_trigger or
                 (action == 'init' and not last_successful)):
            queued = queue_pipeline(ps, spec,
                                    'dirty-task-%s' % action
                                    if completed_trigger is None
                                    else completed_trigger)
            if queued:
                executed_count += 1
                if executed_count == 4 and action == 'update':
                    # Limit ops on update only
                    break

    if executed_count == 0 and action == 'update':
        extra_pipelines = status_all_pipeline_ids.difference(all_pipeline_ids)
        for pipeline_id in extra_pipelines:
            logging.info("Removing Pipeline: %s", pipeline_id)
            status_mgr().deregister(pipeline_id)
            dm.remove(pipeline_id)


@celery_app.task
def execute_scheduled_pipeline(pipeline_id):
    for spec in pipelines([pipeline_id]):
        if spec.pipeline_id == pipeline_id:
            logging.info('Running scheduled pipeline %s, with schedule %s (%r)',
                         pipeline_id, spec.schedule,
                         spec.pipeline_details.get('schedule'))
            update_pipelines.delay('scheduled', pipeline_id, 'scheduled')


@celery_app.task
def execute_pipeline_task(pipeline_id,
                          pipeline_details,
                          pipeline_cwd,
                          trigger,
                          execution_id):

    spec = PipelineSpec(pipeline_id=pipeline_id,
                        pipeline_details=pipeline_details,
                        path=pipeline_cwd)
    success, _, _ = \
        execute_pipeline(spec,
                         execution_id,
                         trigger,
                         False)

    if success:
        has_dependents = len(get_dep_mgr().get_dependents(spec.pipeline_id)) > 0
        logging.info('HAS DEPENDENTS %s: %s', pipeline_id, has_dependents)
        if has_dependents:
            update_pipelines.delay('complete', pipeline_id, trigger)
