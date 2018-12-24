# -*- coding: utf-8 -*-

import copy
import pydash as _

from elasticsearch.exceptions import ElasticsearchException

from common.status_code import ES_OUTOFRESULTWINDOW

from model import models, BaseModel

from module.elasticsearch.const import base


def bulk_error_process(items):
    errors = []
    for item in items:
        for value in item.values():
            if _.get(value, 'error'):
                errors.append(value)
    return errors


def bulk_error_2_elasticsearch_exception(items):
    return ElasticsearchException(bulk_error_process(items))


def list_projection(data, projection):
    if not isinstance(data, list):
        raise Exception('data is not a list')
    result = []
    for d in data:
        result.append(dict_projection(d, projection))

    return result


def dict_projection(data, projection):
    if not projection:
        return data

    _1 = False

    include = {}

    if not isinstance(data, dict):
        raise Exception('data is not a dict')
    if not isinstance(projection, dict):
        raise Exception('projection is not a dict')

    keys = _.keys(data)
    for k, v in projection.items():
        if v:
            _1 = True
        i = _.find_key(keys, lambda x: x == k or _.starts_with(x, k + '.'))
        if i > -1:
            if v:
                include[keys[i]] = _.get(data, keys[i]) or data[keys[i]]
            else:
                data.pop(k)

    if _1:
        return include
    else:
        return data


def dict_filter(data, _filter, ns):
    """
        判断数据是否符合筛选器, 支持mongo的filter语法, 支持两种模式
            1. 简单模式: {key:value}, 直接判断处理
            2. 复杂模式: filter中存在复杂语义, 直接通过数据库进行查询
    """
    if data and _filter:
        for key, value in _filter.items():
            if key.find('$') > -1 or key.find('{') > -1 or str(value).find('$') > -1 or str(value).find('{') > -1:
                # 复杂语义
                return models.get_collection(ns).count(_filter) > 0

            # 简单语义
            v = _.get(data, key)
            if isinstance(v, list):
                if not _.includes(v, value):
                    return False
            elif isinstance(v, type(value)):
                if v != value:
                    return False

    return True


def obj_from_oplog(data, _filter, projection=None, pop_fields=None):
    _id = None
    obj = {}

    _filter = _filter or {}

    op = _.get(data, 'op')
    ns = '.'.join(_.get(data, 'ns').split('.')[1:])
    if op == 'i':
        i_obj = copy.deepcopy(_.get(data, 'o') or {})
        _id = _.get(i_obj, '_id', None)
        if i_obj and dict_filter(i_obj, _filter, ns):
            i_obj.pop('_id', None)
            obj = i_obj
    elif op == 'u':
        _id = _.get(data, 'o2._id')
        u_obj = copy.deepcopy(_.get(data, 'o') or {})
        if u_obj:
            if not _.get(u_obj, '$set'):
                if u_obj and dict_filter(u_obj, _filter, ns):
                    obj = u_obj
                    obj.pop('_id', None)
            else:
                U_filter = dict(_filter, **{'_id': _id})
                collection = models.get_collection(ns)
                if collection.count(U_filter) > 0:
                    obj = _.get(u_obj, '$set')
                    obj.pop('_id', None)
    elif op == 'd':
        _id = _.get(data, 'o._id')

    if _id and pop_fields:
        collection = models.get_collection(ns)
        collection.__class__ = BaseModel
        obj = collection.populate_one(filter={'_id': _id}, projection=projection, pop_fields=pop_fields)
        obj.pop('_id',None)

    return str(_id) if _id else None, obj or {}


def values(obj, mapping=None):
    if mapping and isinstance(mapping, list):
        r = []
        for m in mapping:
            r.append(_.get(obj, m))
        return r
    else:
        return _.values(obj)


def map_(_list, mapping=None, add_title=False):
    if mapping and isinstance(mapping, list):
        r = []

        if add_title:
            title = []
            for m in mapping:
                title.append(_.get(m, 'title'))
            r.append(title)

        for obj in _list:
            item = []
            for m in mapping:
                d = _.get(obj, _.get(m, 'key'))
                item.append(d if d is not None else _.get(m, 'default'))
            r.append(item)
        return r
    else:
        return _.map_(_list)


def pager_builder(page_size=None, page_count=None):
    _size = page_size or base.page_size
    _size = _size if _size <= base.page_max_size else base.page_max_size

    _count = page_count or base.page_count
    _from = _size * (_count - 1)

    if _from + _size > 10000:
        raise ES_OUTOFRESULTWINDOW

    return _size, _from


def scroll_builder(scroll_duration=None, scroll_size=None):
    _scroll = scroll_duration or base.scroll_duration
    _size = scroll_size or base.scroll_size
    return _scroll, _size
