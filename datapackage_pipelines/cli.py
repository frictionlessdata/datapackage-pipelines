import sys

from .manager import execute_pipeline
from .manager.specs import pipelines

def main():
    if len(sys.argv) > 1:
        for pipeline_id, pipeline_details in pipelines():
            if pipeline_id == sys.argv[1]:
                execute_pipeline(pipeline_id, pipeline_details['pipeline'])
    else:
        print('Available Pipelines:')
        for pipeline_id, _ in pipelines():
            print('- {}'.format(pipeline_id))

if __name__ == "__main__":
    main()
