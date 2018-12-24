# -*- coding: utf-8 -*-

import arrow
import pydash as _

from module.elasticsearch.const import base


def search_date_range_min_max_builder(day=None):
    day = day or {'start': None, 'end': None}
    # 没有起始日期默认最小日期
    day['start'] = (arrow.get(day.get('start'))
                    if day.get('start')
                    else base.es_date_min).format(base.arrow_date_format)
    # 没有结束日期默认最大日期
    day['end'] = (arrow.get(day.get('end'))
                  if day.get('end')
                  else base.es_date_max).format(base.arrow_date_format)
    return day


def search_date_range_builder(day=None):
    day = day or {'start': None, 'end': None}
    # 没有起始日期默认本月第一天
    day['start'] = (arrow.get(day.get('start'))
                    if day.get('start')
                    else arrow.now().replace(day=1)).format(base.arrow_date_format)
    # 结束日期默认为参数中结束日期加一天, 保障包含结束日期
    day['end'] = (arrow.get(day.get('end'))
                  if day.get('end')
                  else arrow.now()).replace(days=1).format(base.arrow_date_format)
    return day


def date_time_format(date):
    return arrow.get(date).to('local').format(base.arrow_time_format) if date else ''


def query_builder(mapping, **kwargs):
    _query = []

    map = key_builder(mapping)

    for k, v in map.items():
        val = _.get(kwargs, k)
        if val or val == 0:
            if isinstance(val, list):
                _query.append({'terms': {v: val}})  # support list value
            else:
                if _.starts_with(k, 'regexp_'):
                    _query.append({'regexp': {v: val}})  # support normal value
                else:
                    _query.append({'term': {v: val}})  # support normal value

    return _query


keys = []


def key_builder(es_field_mapping):
    map = {}
    for k, v in es_field_mapping.items():
        keys.append(k)
        if isinstance(v, str):
            map['.'.join(keys)] = v
        else:
            map = _.assign(map, key_builder(v))

        keys.pop()
    return map


def agg_builder(mapping, agg_keys):
    current = {}
    child = None
    for k in reversed(agg_keys):
        current = {k: _.get(mapping, k + '.agg_cmd')}
        if current:
            if child:
                current = _.set_(current, k + ".aggs", child)
            child = _.clone_deep(current)

    return current


def get_buckets_from_aggregation(aggregations, mapping, agg_keys, level):
    if not aggregations or not mapping or not agg_keys:
        return []
    bucket_key = _.get(mapping, agg_keys[level] + '.bucket_key', 'key')
    if isinstance(bucket_key, str):
        bucket_key = [bucket_key]
    result = _.get(aggregations, agg_keys[level] + '.buckets')
    if level >= len(agg_keys) - 1:
        r = []
        if result:
            for item in result:
                a = {}
                a['count'] = item['doc_count']
                for bk in bucket_key:
                    key_get = 'key'
                    key_set = 'key'
                    if isinstance(bk, str):
                        key_get = bk
                        key_set = agg_keys[level]
                    elif isinstance(bk, dict):
                        key_get = _.get(bk, 'get')
                        key_set = _.get(bk, 'set')
                    _.set_(a, key_set, _.get(item, key_get))
                r.append(a)
        return r
    else:
        _list = []
        for item in result:
            for child in get_buckets_from_aggregation(item, mapping, agg_keys, level + 1):
                for bk in bucket_key:
                    key_get = 'key'
                    key_set = 'key'
                    if isinstance(bk, str):
                        key_get = bk
                        key_set = agg_keys[level]
                    elif isinstance(bk, dict):
                        key_get = _.get(bk, 'get')
                        key_set = _.get(bk, 'set')
                    child = _.assign(child, {key_set: item.get(key_get)})
                _list.append(child)
        return _list
