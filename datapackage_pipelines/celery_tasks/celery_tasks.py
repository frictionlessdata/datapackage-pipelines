from .celery_app import celery_app
from datapackage_pipelines.manager.tasks import execute_pipeline

@celery_app.task
def execute_pipeline_task(pipeline_id, pipeline_steps, pipeline_cwd):
    execute_pipeline(pipeline_id, pipeline_steps, pipeline_cwd, 'schedule')
