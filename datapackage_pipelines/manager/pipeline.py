from .specs import pipelines

TASK_NAME = 'datapackage_pipelines.manager.tasks.execute_pipeline_task'


def initialize_pipeline():

    def schedule_entries():
        for pipeline_id, pipeline_details, pipeline_cwd in pipelines():
            entry = {
                'task': TASK_NAME,
                'schedule': pipeline_details['schedule'],
                'args': (pipeline_id, pipeline_details['pipeline'], pipeline_cwd)
            }
            yield pipeline_id, entry

    return dict(schedule_entries())
