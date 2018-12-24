# -*- coding: utf-8 -*-

import pydash as _

from elasticsearch.exceptions import ElasticsearchException


def bulk_error_process(items):
    errors = []
    for item in items:
        for value in item.values():
            if _.get(value, 'error'):
                errors.append(value)
    return errors


def bulk_error_2_elasticsearch_exception(items):
    return ElasticsearchException(bulk_error_process(items))
