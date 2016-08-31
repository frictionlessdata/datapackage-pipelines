from .specs import pipelines

TASK_NAME = 'datapackage_pipelines.manager.tasks.execute_pipeline_task'


def initialize_pipeline():

    def schedule_entries():
        for pipline_id, pipeline_details in pipelines():
            entry = {
                'task': TASK_NAME,
                'schedule': pipeline_details['schedule'],
                'args': (pipline_id, pipeline_details['pipeline'])
            }
            yield pipline_id, entry

    return dict(schedule_entries())
