import datetime
import json as _json

import decimal


class CommonJSONDecoder(_json.JSONDecoder):
    """
    Common JSON Encoder
    json.loads(myString, cls=CommonJSONEncoder)
    """

    @classmethod
    def object_hook(cls, obj):  # pylint: disable=method-hidden
        if 'type{decimal}' in obj:
            try:
                return decimal.Decimal(obj['type{decimal}'])
            except decimal.InvalidOperation:
                pass
        if 'type{date}' in obj:
            try:
                return datetime.datetime \
                    .strptime(obj["type{date}"], '%Y-%m-%d') \
                    .date()
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

    def default(self, obj):     # pylint: disable=method-hidden

        if isinstance(obj, decimal.Decimal):
            return {'type{decimal}': str(obj)}
        elif isinstance(obj, datetime.date):
            return {'type{date}': str(obj)}


def _dumps(*args, **kwargs):
    kwargs['cls'] = CommonJSONEncoder
    return _json.dumps(*args, **kwargs)


def _loads(*args, **kwargs):
    kwargs['cls'] = CommonJSONDecoder
    return _json.loads(*args, **kwargs)


class json(object):
    dumps = _dumps
    loads = _loads