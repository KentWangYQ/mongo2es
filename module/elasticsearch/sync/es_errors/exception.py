# -*- coding: utf-8 -*-

import pydash as _
import arrow
from common.constant import ErrorStatus
from common.elasticsearch.elasticsearch_client.elasticsearch_client import ElasticsearchClient
from module.elasticsearch.const import structure
from module.elasticsearch.sync import es_sync_util

opt = {
    'index': structure.es_errors['index'],
    'type': structure.es_errors['type']['exception'],
    'mappings': structure.es_errors['mappings'],
    'settings': structure.es_errors['settings'],
}

es_client = None


def index():
    es_sync_util.index_exists_create(index=opt['index'], mappings=opt['mappings'], settings=opt['settings'])


def index_one(exception, trace=None, es_client=es_client):
    if not es_client:
        es_client = ElasticsearchClient().client
    body = {
        'message': _.get(exception, 'info') or _.get(exception, 'message'),
        'created': arrow.utcnow().datetime,
        'status': ErrorStatus.open,
        'trace': trace
    }
    es_client.index(index=opt['index'], doc_type=opt['type'], body=body)
