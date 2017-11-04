# -*- coding: utf-8 -*-
import json
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

from datapackage_pipelines.manager import execute_pipeline
from datapackage_pipelines.specs.specs import pipelines
from datapackage_pipelines.utilities.execution_id import gen_execution_id
from datapackage_pipelines.status import status


called_hooks = []
progresses = 0

class SaveHooks(BaseHTTPRequestHandler):

    def do_POST(self):
        global progresses
        content_len = int(self.headers.get('content-length', 0))
        post_body = self.rfile.read(content_len)
        hook = json.loads(post_body)
        if hook['event'] != 'progress':
            called_hooks.append(hook)
        else:
            progresses += 1
        self.send_response(200)
        self.end_headers()
        return


def test_pipeline():
    '''Tests a few pipelines.'''
    global progresses

    server = HTTPServer(('', 9000), SaveHooks)
    thread = threading.Thread(target = server.serve_forever, daemon=True)
    thread.start()

    for spec in pipelines():
        if spec.pipeline_id.startswith('./tests/env/dummy/pipeline-test'):
            eid = gen_execution_id()
            status.get(spec.pipeline_id).queue_execution(eid, 'manual')
            success, _, _ = execute_pipeline(spec, eid, use_cache=False)
            assert success

    assert len(called_hooks) == 3
    assert called_hooks == [
        {"pipeline_id": "./tests/env/dummy/pipeline-test-hooks", "event": "queue"},
        {"pipeline_id": "./tests/env/dummy/pipeline-test-hooks", "event": "start"},
        {"pipeline_id": "./tests/env/dummy/pipeline-test-hooks", "event": "finish", "success": True,
         'stats': {'.dpp': {'out-datapackage-url': 'hooks-outputs/datapackage.json'},
                   'bytes': 258, 'count_of_rows': None,
                   'dataset_name': 'hook-tests', 'hash': 'f3f25f5ecd8e7e2c35d83139178072b8'}}
    ]
    assert progresses >= 1