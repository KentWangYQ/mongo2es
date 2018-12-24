# -*- coding: utf-8 -*-

import arrow
import pydash as _
from common.elasticsearch.elasticsearch_client.elasticsearch_client import es_client, ElasticsearchClient
from module.elasticsearch.const import structure, base
from module.elasticsearch import util

opt = {
    'index': structure.es_errors['index'],
    'type': structure.es_errors['type']['exception']
}


def ___query_builder_for_search(**kwargs):
    _size, _from = util.pager_builder(kwargs.get('page_size'), kwargs.get('page_count'))

    es_term_mapping = {
        'status': 'status',
        'error_code': 'message.status',
        'error_type': 'message.error.type.keyword'
    }

    _query = []

    for l1 in es_term_mapping:
        if kwargs.get(l1):
            _field = _.get(es_term_mapping, l1)
            _value = _.get(kwargs, l1)
            if _field and _value:
                _query.append({'term': {_field: _value}})
    r = {
        'time_zone': base.es_time_zone,
        'format': base.es_time_format
    }
    if kwargs.get('start'):
        r['gte'] = arrow.get(kwargs.get('start')).format(base.arrow_time_format)
    if kwargs.get('end'):
        r['lt'] = arrow.get(kwargs.get('end')).format(base.arrow_time_format)

    _query.append({
        "range": {
            "created": r
        }
    })

    return _size, _from, _query


def change_status(error_id, status):
    ElasticsearchClient().client.update(index=opt['index'], doc_type=opt['type'], id=error_id,
                                        body={'doc': {'status': status}})


def delete_error(error_id):
    ElasticsearchClient().client.delete(index=opt['index'], doc_type=opt['type'], id=error_id)


def search(**kwargs):
    """
    查询ES错误信息, 含数据同步过程中出现的数据问题
    :param kwargs:
            status: 'open'
            error_code: 400
            error_type: 'mapper_parsing_exception'
            start: '2018-05-18 00:00:00'
            end: '2018-05-19 01:02:03',
            page_size: 20,
            page_count: 1
    :return:
    """
    _size, _from, _query = ___query_builder_for_search(**kwargs)
    body = {
        "size": _size,
        "from": _from,
        "query": {
            "bool": {
                "must": _query
            }
        }
    }
    return ElasticsearchClient().client.search(index=opt['index'], doc_type=opt['type'], body=body)
