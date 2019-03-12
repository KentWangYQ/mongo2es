# -*- coding: utf-8 -*-

import bson
from bson import json_util

# ##### override bson.json_util.default ##### #
_old_default = json_util.default


def _default(obj, json_options=json_util.DEFAULT_JSON_OPTIONS):
    # override时间处理方法，格式由{$date:{datetime}}，转换为datetime
    res = _old_default(obj=obj, json_options=json_options)

    if isinstance(obj, json_util.datetime.datetime):
        if res['$date']:
            return res['$date']

    if isinstance(obj, bson.ObjectId):
        return str(obj)

    return res


json_util.default = _default

# ##### override bson.json_util.dump ##### #
_old_dumps = json_util.dumps


def _dumps(obj, *args, **kwargs):
    json_options = kwargs.pop("json_options", json_util.JSONOptions(json_mode=json_util.JSONMode.RELAXED))
    return _old_dumps(obj, json_options=json_options, *args, **kwargs)


def _decode_dumps(obj, *args, **kwargs):
    encoding = kwargs.pop('encode', 'unicode_escape')
    indent = kwargs.pop('indent', 2)
    return _dumps(obj, indent=indent, *args, **kwargs).decode(encoding=encoding)


json_util.dumps = _dumps
json_util.decode_dumps = _decode_dumps
