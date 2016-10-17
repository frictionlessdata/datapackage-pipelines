import datetime

from flask import Flask, render_template, abort, redirect

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
            else 'Failed'
        pipeline['slug'] = pipeline['id'].replace('/', '_').replace('.', '_')
    return render_template('dashboard.html', pipelines=statuses)


@app.route("/badge/<path:pipeline_id>")
def badge(pipeline_id):
    if not pipeline_id.startswith('./'):
        pipeline_id = './' + pipeline_id
    pipeline_status = status.get_status(pipeline_id)
    if pipeline_status is None:
        abort(404)
    success = pipeline_status.get('success')
    if success is True:
        status_text = 'Passing'
        status_color = 'brightgreen'
    elif success is False:
        status_text = 'Failing'
        status_color = 'red'
    else:
        status_text = "Haven't run"
        status_color = 'lightgray'
    return redirect('https://img.shields.io/badge/{}-{}-{}.svg'.format(
        'pipeline', status_text, status_color
    ))
