import sys

from .manager import execute_pipeline
from .manager.specs import pipelines


def main():
    if len(sys.argv) > 1:
        if sys.argv[1] == 'serve':
            from .web import app
            app.run(host='0.0.0.0', debug=True, port=5000)
        else:
            for pipeline_id, pipeline_details, pipeline_cwd, dirty \
                    in pipelines():
                if pipeline_id == sys.argv[1]:
                    execute_pipeline(pipeline_id,
                                     pipeline_details['pipeline'],
                                     pipeline_cwd)
                    break
    else:
        print('Available Pipelines:')
        for pipeline_id, _, _, dirty in pipelines():
            print('- {} {}'.format(pipeline_id, '(*)' if dirty else ''))

if __name__ == "__main__":
    main()
