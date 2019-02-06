import datetime
import os
from io import BytesIO
import logging
from functools import wraps
from copy import deepcopy
from collections import Counter

import slugify
import yaml
import mistune
import requests

from flask import \
    Blueprint, Flask, render_template, abort, send_file, make_response
from flask_cors import CORS
from flask_jsonpify import jsonify
from flask_basicauth import BasicAuth

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


def basic_auth_required(view_func):
    """
    A decorator that can be used to protect specific views with HTTP basic
    access authentication. Conditional on having BASIC_AUTH_USERNAME and
    BASIC_AUTH_PASSWORD set as env vars.
    """
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if app.config.get('BASIC_AUTH_ACTIVE', False):
            if basic_auth.authenticate():
                return view_func(*args, **kwargs)
            else:
                return basic_auth.challenge()
        else:
            return view_func(*args, **kwargs)
    return wrapper


blueprint = Blueprint('dpp', 'dpp')


@blueprint.route("")
@blueprint.route("<path:pipeline_path>")
@basic_auth_required
def main(pipeline_path=None):
    pipeline_ids = sorted(status.all_pipeline_ids())

    # If we have a pipeline_path, filter the pipeline ids.
    if pipeline_path is not None:
        if not pipeline_path.startswith('./'):
            pipeline_path = './' + pipeline_path

        pipeline_ids = [p for p in pipeline_ids if p.startswith(pipeline_path)]

    statuses = []
    for pipeline_id in pipeline_ids:
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
            'last_success':
                datestr(success_ex.finish_time) if success_ex else None,
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


@blueprint.route("api/raw/status")
@basic_auth_required
def pipeline_raw_api_status():
    pipelines = sorted(status.all_statuses(), key=lambda x: x.get('id'))
    for pipeline in pipelines:
        # can get the full details from api/raw/<path:pipeline_id>
        for attr in ["pipeline", "reason", "error_log"]:
            if attr in pipeline:
                del pipeline[attr]
    return jsonify(pipelines)


@blueprint.route("api/raw/<path:pipeline_id>")
@basic_auth_required
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
        "last_success":
            last_successful_execution.finish_time
            if last_successful_execution else None,
        "trigger": last_execution.trigger if last_execution else None,

        "pipeline": pipeline_status.pipeline_details,
        "source": pipeline_status.source_spec,
        "message": pipeline_status.state().capitalize(),
        "state": pipeline_status.state(),
    }

    return jsonify(ret)


@blueprint.route("api/<field>/<path:pipeline_id>")
@basic_auth_required
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


def _make_badge_response(subject, text, colour):
    image_url = 'https://img.shields.io/badge/{}-{}-{}.svg'.format(
        subject, text, colour)
    r = requests.get(image_url)
    buffer_image = BytesIO(r.content)
    buffer_image.seek(0)
    res = make_response(send_file(buffer_image, mimetype='image/svg+xml'))
    res.headers['Cache-Control'] = \
        'max-age=0, no-cache, no-store, must-revalidate'
    res.headers['Expires'] = '0'
    return res


@blueprint.route("badge/<path:pipeline_id>")
def badge(pipeline_id):
    '''An individual pipeline status'''
    if not pipeline_id.startswith('./'):
        pipeline_id = './' + pipeline_id
    pipeline_status = status.get(pipeline_id)

    status_color = 'lightgray'
    if pipeline_status.pipeline_details:
        status_text = pipeline_status.state().lower()
        last_execution = pipeline_status.get_last_execution()
        success = last_execution.success if last_execution else None
        if success is True:
            stats = last_execution.stats if last_execution else None
            record_count = stats.get('count_of_rows')
            if record_count is not None:
                status_text += ' (%d records)' % record_count
            status_color = 'brightgreen'
        elif success is False:
            status_color = 'red'
    else:
        status_text = "not found"
    return _make_badge_response('pipeline', status_text, status_color)


@blueprint.route("badge/collection/<path:pipeline_path>")
def badge_collection(pipeline_path):
    '''Status badge for a collection of pipelines.'''
    all_pipeline_ids = sorted(status.all_pipeline_ids())

    if not pipeline_path.startswith('./'):
        pipeline_path = './' + pipeline_path

    # Filter pipeline ids to only include those that start with pipeline_path.
    path_pipeline_ids = \
        [p for p in all_pipeline_ids if p.startswith(pipeline_path)]

    statuses = []
    for pipeline_id in path_pipeline_ids:
        pipeline_status = status.get(pipeline_id)
        if pipeline_status is None:
            abort(404)
        status_text = pipeline_status.state().lower()
        statuses.append(status_text)

    status_color = 'lightgray'
    status_counter = Counter(statuses)
    if status_counter:
        if len(status_counter) == 1 and status_counter['succeeded'] > 0:
            status_color = 'brightgreen'
        elif status_counter['failed'] > 0:
            status_color = 'red'
        elif status_counter['failed'] == 0:
            status_color = 'yellow'
        status_text = \
            ', '.join(['{} {}'.format(v, k)
                       for k, v in status_counter.items()])
    else:
        status_text = "not found"

    return _make_badge_response('pipelines', status_text, status_color)


app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

if os.environ.get('DPP_BASIC_AUTH_USERNAME', False) \
   and os.environ.get('DPP_BASIC_AUTH_PASSWORD', False):
    app.config['BASIC_AUTH_USERNAME'] = os.environ['DPP_BASIC_AUTH_USERNAME']
    app.config['BASIC_AUTH_PASSWORD'] = os.environ['DPP_BASIC_AUTH_PASSWORD']
    app.config['BASIC_AUTH_ACTIVE'] = True

basic_auth = BasicAuth(app)


CORS(app)

url_prefix = os.environ.get('DPP_BASE_PATH', '/')
if not url_prefix.endswith('/'):
    url_prefix += '/'
logging.info('Serving on path %s', url_prefix)

app.register_blueprint(blueprint, url_prefix=url_prefix)
