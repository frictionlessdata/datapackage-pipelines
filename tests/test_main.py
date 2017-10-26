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


class SaveHooks(BaseHTTPRequestHandler):

    def do_POST(self):
        content_len = int(self.headers.get('content-length', 0))
        post_body = self.rfile.read(content_len)
        called_hooks.append(json.loads(post_body))
        self.send_response(200)
        self.end_headers()
        return


def test_pipeline():
    '''Tests a few pipelines.'''

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
        {"pipeline_id": "./tests/env/dummy/pipeline-test-hooks", "event": "finish", "success": True}
    ]