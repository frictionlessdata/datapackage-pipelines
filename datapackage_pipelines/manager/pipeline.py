from .tasks import execute_pipeline_task
from .specs import pipelines

TASK_NAME = 'datapackage_pipelines.manager.tasks.execute_pipeline_task'


def initialize_pipeline():

    def schedule_entries():
        for pipeline_id, pipeline_details, pipeline_cwd, dirty \
                in pipelines():
            entry = {
                'task': TASK_NAME,
                'schedule': pipeline_details['schedule'],
                'args': (pipeline_id,
                         pipeline_details['pipeline'],
                         pipeline_cwd)
            }
            if dirty:
                execute_pipeline_task.delay(pipeline_id,
                                            pipeline_details,
                                            pipeline_cwd)
            yield pipeline_id, entry

    return dict(schedule_entries())
