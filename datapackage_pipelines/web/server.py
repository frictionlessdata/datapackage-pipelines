import datetime

from flask import Flask, render_template
from ..manager.status import status

app = Flask(__name__)


def datestr(x):
    return str(datetime.datetime.fromtimestamp(x))


@app.route("/")
def main():
    statuses = status.all_statuses()
    for pipeline in statuses:
        for key in ['ended', 'last_success', 'started']:
            if pipeline.get(key):
                pipeline[key] = datestr(pipeline[key])
        pipeline['class'] = 'warning' if pipeline['running'] else \
            'success' if pipeline.get('success') \
                else 'danger'
        pipeline['status'] = 'Running' if pipeline['running'] else \
            'Idle' if pipeline.get('success') \
                else 'Errored'
        pipeline['slug'] = pipeline['id'].replace('/', '_').replace('.', '_')
    return render_template('dashboard.html', pipelines=statuses)
