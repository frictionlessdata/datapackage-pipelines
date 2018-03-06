import datetime
import os

import logging
import slugify
import yaml
import mistune
from copy import deepcopy
from flask import Blueprint

from flask import Flask, render_template, abort, redirect
from flask_cors import CORS
from flask_jsonpify import jsonify

from datapackage_pipelines.celery_tasks.celery_tasks import execute_update_pipelines
from datapackage_pipelines.status import status_mgr
from datapackage_pipelines.utilities.stat_utils import user_facing_stats

YAML_DUMPER = yaml.CDumper if 'CDumper' in yaml.__dict__ else yaml.Dumper


def datestr(x):
    if x is None:
        return ''
    return str(datetime.datetime.fromtimestamp(x))


def yamlize(x):
    ret = yaml.dump(x, default_flow_style=False, Dumper=YAML_DUMPER)
    return ret


markdown = mistune.Markdown(hard_wrap=True)
status = status_mgr()


def make_hierarchies(statuses):

    def group(l):
        pipelines = list(filter(lambda x: len(x['id']) == 1, l))
        children_ = list(filter(lambda x: len(x['id']) > 1, l))
        groups_ = {}
        for child in children_:
            child_key = child['id'].pop(0)
            groups_.setdefault(child_key, []).append(child)
        children_ = dict(
            (k, group(v))
            for k, v in groups_.items()
        )
        for p in pipelines:
            p['id'] = p['id'][0]
        return {
            'pipelines': pipelines,
            'children': children_
        }

    def flatten(children_):
        for k, v in children_.items():
            v['children'] = flatten(v['children'])
            child_keys = list(v['children'].keys())
            if len(child_keys) == 1 and len(v['pipelines']) == 0:
                child_key = child_keys[0]
                children_['/'.join([k, child_key])] = v['children'][child_key]
                del children_[k]
        return children_

    statuses = [
       {
            'id': st['id'].split('/'),
            'title': st.get('title'),
            'stats': st.get('stats'),
            'slug': st.get('slug')
       }
       for st in statuses
    ]
    groups = group(statuses)
    children = groups.get('children', {})
    groups['children'] = flatten(children)

    return groups


blueprint = Blueprint('dpp', 'dpp')


@blueprint.route("")
def main():
    all_pipeline_ids = sorted(status.all_pipeline_ids())
    statuses = []
    for pipeline_id in all_pipeline_ids:
        pipeline_status = status.get(pipeline_id)
        ex = pipeline_status.get_last_execution()
        success_ex = pipeline_status.get_last_successful_execution()
        pipeline_obj = {
            'id': pipeline_id.lstrip('./'),
            'title': pipeline_status.pipeline_details.get('title'),
            'stats': user_facing_stats(ex.stats) if ex else None,
            'slug': slugify.slugify(pipeline_id),
            'trigger': ex.trigger if ex else None,
            'error_log': pipeline_status.errors(),
            'state': pipeline_status.state(),
            'pipeline': pipeline_status.pipeline_details,
            'message': pipeline_status.state().capitalize(),
            'dirty': pipeline_status.dirty(),
            'runnable': pipeline_status.runnable(),
            'class': {'INIT': 'primary',
                      'QUEUED': 'primary',
                      'INVALID': 'danger',
                      'RUNNING': 'warning',
                      'SUCCEEDED': 'success',
                      'FAILED': 'danger'
                      }[pipeline_status.state()],
            'ended': datestr(ex.finish_time) if ex else None,
            'started': datestr(ex.start_time) if ex else None,
            'last_success': datestr(success_ex.finish_time) if success_ex else None,
        }
        statuses.append(pipeline_obj)

    def state_and_not_dirty(state, p):
        return p.get('state') == state and not p.get('dirty')

    def state_or_dirty(state, p):
        return p.get('state') == state or p.get('dirty')

    categories = [
        ['ALL', 'All Pipelines', lambda _, __: True],
        ['INVALID', "Can't start", lambda _, p: not p['runnable']],
        ['QUEUED', 'Waiting to run', lambda state, p: p['state'] == state],
        ['RUNNING', 'Running', state_and_not_dirty],
        ['FAILED', 'Failed Execution', state_and_not_dirty],
        ['SUCCEEDED', 'Successful Execution', state_and_not_dirty],
    ]
    for item in categories:
        item.append([p for p in deepcopy(statuses)
                     if item[2](item[0], p)])
        item.append(len(item[-1]))
        item.append(make_hierarchies(item[-2]))
    return render_template('dashboard.html',
                           categories=categories,
                           yamlize=yamlize,
                           markdown=markdown)


@blueprint.route("api/refresh")
def refresh():
    execute_update_pipelines()
    return jsonify({'ok': True})


@blueprint.route("api/raw/status")
def pipeline_raw_api_status():
    pipelines = sorted(status.all_statuses(), key=lambda x: x.get('id'))
    for pipeline in pipelines:
        # can get the full details from api/raw/<path:pipeline_id>
        for attr in ["pipeline", "reason", "error_log"]:
            if attr in pipeline:
                del pipeline[attr]
    return jsonify(pipelines)


@blueprint.route("api/raw/<path:pipeline_id>")
def pipeline_raw_api(pipeline_id):
    if not pipeline_id.startswith('./'):
        pipeline_id = './' + pipeline_id
    pipeline_status = status.get(pipeline_id)
    if not pipeline_status.pipeline_details:
        abort(404)
    last_execution = pipeline_status.get_last_execution()
    last_successful_execution = pipeline_status.get_last_successful_execution()
    ret = {
        "id": pipeline_id,
        "cache_hash": pipeline_status.cache_hash,
        "dirty": pipeline_status.dirty(),

        "queued": last_execution.queue_time if last_execution else None,
        "started": last_execution.start_time if last_execution else None,
        "ended": last_execution.finish_time if last_execution else None,
        "reason": last_execution.log if last_execution else None,
        "error_log": pipeline_status.errors(),
        "stats": last_execution.stats if last_execution else None,
        "success": last_execution.success if last_execution else None,
        "last_success": last_successful_execution.finish_time if last_successful_execution else None,
        "trigger": last_execution.trigger if last_execution else None,

        "pipeline": pipeline_status.pipeline_details,
        "source": pipeline_status.source_spec,
        "message": pipeline_status.state().capitalize(),
        "state": pipeline_status.state(),
    }

    return jsonify(ret)


@blueprint.route("api/<field>/<path:pipeline_id>")
def pipeline_api(field, pipeline_id):

    if not pipeline_id.startswith('./'):
        pipeline_id = './' + pipeline_id
    pipeline_status = status.get(pipeline_id)
    if not pipeline_status.pipeline_details:
        abort(404)

    ret = None
    if field == 'pipeline':
        ret = pipeline_status.pipeline_details
        ret = yamlize(ret)
    elif field == 'source':
        ret = pipeline_status.source_spec
        ret = yamlize(ret)
    elif field == 'log':
        ex = pipeline_status.get_last_execution()
        ret = ex.log if ex else ''
    else:
        abort(400)

    ret = ret.split('\n')
    ret = {'text': ret}
    return jsonify(ret)


@blueprint.route("badge/<path:pipeline_id>")
def badge(pipeline_id):
    if not pipeline_id.startswith('./'):
        pipeline_id = './' + pipeline_id
    pipeline_status = status.get_status(pipeline_id)
    if pipeline_status is None:
        abort(404)
    status_text = pipeline_status.get('message')
    success = pipeline_status.get('success')
    if success is True:
        record_count = pipeline_status.get('stats', {}).get('total_row_count')
        if record_count is not None:
            status_text += ' (%d records)' % record_count
        status_color = 'brightgreen'
    elif success is False:
        status_color = 'red'
    else:
        status_color = 'lightgray'
    return redirect('https://img.shields.io/badge/{}-{}-{}.svg'.format(
        'pipeline', status_text, status_color
    ))


app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
CORS(app)

url_prefix = os.environ.get('DPP_BASE_PATH', '/')
if not url_prefix.endswith('/'):
    url_prefix += '/'
logging.info('Serving on path %s', url_prefix)

app.register_blueprint(blueprint, url_prefix=url_prefix)
