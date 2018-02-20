import datetime
import json as _json

import decimal
import isodate

from .lazy_dict import LazyDict


DATE_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
TIME_FORMAT = '%H:%M:%S'


class LazyJsonLine(LazyDict):

    def __init__(self, args, kwargs):
        super().__init__()
        self.line = args[0]
        self.args = args
        self.kwargs = kwargs

    def _evaluate(self):
        return json.loads(*self.args, **self.kwargs)

    def __str__(self):
        if self.inner is not None:
            return str(self.inner)
        return self.line

    def __repr__(self):
        if self.inner is not None:
            return repr(self.inner)
        return self.line


class CommonJSONDecoder(_json.JSONDecoder):
    """
    Common JSON Encoder
    json.loads(myString, cls=CommonJSONEncoder)
    """

    @classmethod
    def object_hook(cls, obj):
        if 'type{decimal}' in obj:
            try:
                return decimal.Decimal(obj['type{decimal}'])
            except decimal.InvalidOperation:
                pass
        if 'type{time}' in obj:
            try:
                return datetime.datetime \
                    .strptime(obj["type{time}"], TIME_FORMAT) \
                    .time()
            except ValueError:
                pass
        if 'type{datetime}' in obj:
            try:
                return datetime.datetime \
                    .strptime(obj["type{datetime}"], DATETIME_FORMAT)
            except ValueError:
                pass
        if 'type{date}' in obj:
            try:
                return datetime.datetime \
                    .strptime(obj["type{date}"], DATE_FORMAT) \
                    .date()
            except ValueError:
                pass
        if 'type{duration}' in obj:
            try:
                return isodate.parse_duration(obj["type{duration}"])
            except ValueError:
                pass
        if 'type{set}' in obj:
            try:
                return set(obj['type{set}'])
            except ValueError:
                pass

        return obj

    def __init__(self, **kwargs):
        kwargs['object_hook'] = self.object_hook
        super(CommonJSONDecoder, self).__init__(**kwargs)


class CommonJSONEncoder(_json.JSONEncoder):
    """
    Common JSON Encoder
    json.dumps(myString, cls=CommonJSONEncoder)
    """

    def default(self, obj):

        if isinstance(obj, decimal.Decimal):
            return {'type{decimal}': str(obj)}
        elif isinstance(obj, datetime.time):
            return {'type{time}': obj.strftime(TIME_FORMAT)}
        elif isinstance(obj, datetime.datetime):
            return {'type{datetime}': obj.strftime(DATETIME_FORMAT)}
        elif isinstance(obj, datetime.date):
            return {'type{date}': obj.strftime(DATE_FORMAT)}
        elif isinstance(obj, (isodate.Duration, datetime.timedelta)):
            return {'type{duration}': isodate.duration_isoformat(obj)}
        elif isinstance(obj, set):
            return {'type{set}': list(obj)}
        elif isinstance(obj, LazyDict):
            return obj.inner
        return super().default(obj)


def _dumpl(*args, **kwargs):
    obj = args[0]
    if isinstance(obj, LazyJsonLine):
        if not obj.dirty:
            return obj.line
        else:
            kwargs['cls'] = CommonJSONEncoder
            return _json.dumps(obj.inner, **kwargs)
    kwargs['cls'] = CommonJSONEncoder
    return _json.dumps(*args, **kwargs)


def _loadl(*args, **kwargs):
    if args[0][0] == '{':
        kwargs['cls'] = CommonJSONDecoder
        return LazyJsonLine(args, kwargs)
    else:
        return _loads(*args, **kwargs)


def _dumps(*args, **kwargs):
    kwargs['cls'] = CommonJSONEncoder
    return _json.dumps(*args, **kwargs)


def _loads(*args, **kwargs):
    kwargs['cls'] = CommonJSONDecoder
    return _json.loads(*args, **kwargs)


def _dump(*args, **kwargs):
    kwargs['cls'] = CommonJSONEncoder
    return _json.dump(*args, **kwargs)


def _load(*args, **kwargs):
    kwargs['cls'] = CommonJSONDecoder
    return _json.load(*args, **kwargs)


class json(object):
    dumps = _dumps
    loads = _loads
    dumpl = _dumpl
    loadl = _loadl
    dump = _dump
    load = _load
    JSONDecodeError = _json.JSONDecodeError
