# -*- coding: utf-8 -*-

import pydash as _

from elasticsearch import Elasticsearch, Transport
from elasticsearch.client.utils import query_params

from config import settings

from common.kafka.producer import Producer
from common.mongo.bson_c import json_util


class ElasticsearchKafkaClient(Elasticsearch):
    def __init__(self, hosts=None, transport_class=Transport, **kwargs):
        self.producer = Producer()
        self.topic = _.get(settings.SYNC, 'rts.queue.key')
        super(ElasticsearchKafkaClient, self).__init__(hosts=hosts, transport_class=transport_class, **kwargs)

    @query_params('parent', 'pipeline', 'refresh', 'routing', 'timeout',
                  'timestamp', 'ttl', 'version', 'version_type', 'wait_for_active_shards')
    def create(self, index, doc_type, id, body, params=None):
        self.producer.send(topic=self.topic,
                           value=json_util.dumps({'op': 'create', 'index': index, 'doc_type': doc_type,
                                                  'body': body, 'id': id, 'params': params}))

    @query_params('op_type', 'parent', 'pipeline', 'refresh', 'routing',
                  'timeout', 'timestamp', 'ttl', 'version', 'version_type',
                  'wait_for_active_shards')
    def index(self, index, doc_type, body, id=None, params=None):
        self.producer.send(topic=self.topic,
                           value=json_util.dumps({'op': 'index', 'index': index, 'doc_type': doc_type,
                                                  'body': body, 'id': id, 'params': params}))

    @query_params('_source', '_source_exclude', '_source_include', 'fields',
                  'lang', 'parent', 'refresh', 'retry_on_conflict', 'routing', 'timeout',
                  'timestamp', 'ttl', 'version', 'version_type', 'wait_for_active_shards')
    def update(self, index, doc_type, id, body=None, params=None):
        self.producer.send(topic=self.topic,
                           value=json_util.dumps({'op': 'update', 'index': index, 'doc_type': doc_type,
                                                  'body': body, 'id': id, 'params': params}))

    @query_params('parent', 'refresh', 'routing', 'timeout', 'version',
                  'version_type', 'wait_for_active_shards')
    def delete(self, index, doc_type, id, params=None):
        self.producer.send(topic=self.topic,
                           value=json_util.dumps(
                               {'op': 'delete', 'index': index, 'doc_type': doc_type, 'id': id, 'params': params}))

    @query_params('_source', '_source_exclude', '_source_include', 'fields',
                  'pipeline', 'refresh', 'routing', 'timeout', 'wait_for_active_shards')
    def bulk(self, body, index=None, doc_type=None, params=None):
        self.producer.send(topic=self.topic,
                           value=json_util.dumps({'op': 'bulk', 'index': index, 'doc_type': doc_type,
                                                  'body': body, 'params': params
                                                  }))
        # 兼容Elasticsearch原生sdk返回结果
        return {'errors': None, 'items': []}

    @query_params('_source', '_source_exclude', '_source_include',
                  'allow_no_indices', 'analyze_wildcard', 'analyzer', 'conflicts',
                  'default_operator', 'df', 'expand_wildcards', 'from_',
                  'ignore_unavailable', 'lenient', 'pipeline', 'preference', 'q',
                  'refresh', 'request_cache', 'requests_per_second', 'routing', 'scroll',
                  'scroll_size', 'search_timeout', 'search_type', 'size', 'slices',
                  'sort', 'stats', 'terminate_after', 'timeout', 'version',
                  'version_type', 'wait_for_active_shards', 'wait_for_completion')
    def update_by_query(self, index, doc_type=None, body=None, params=None):
        self.producer.send(topic=self.topic,
                           value=json_util.dumps({'op': 'update_by_query', 'index': index, 'doc_type': doc_type,
                                                  'body': body, 'params': params
                                                  }))

    @query_params('_source', '_source_exclude', '_source_include',
                  'allow_no_indices', 'analyze_wildcard', 'analyzer', 'conflicts',
                  'default_operator', 'df', 'expand_wildcards', 'from_',
                  'ignore_unavailable', 'lenient', 'preference', 'q', 'refresh',
                  'request_cache', 'requests_per_second', 'routing', 'scroll',
                  'scroll_size', 'search_timeout', 'search_type', 'size', 'slices',
                  'sort', 'stats', 'terminate_after', 'timeout', 'version',
                  'wait_for_active_shards', 'wait_for_completion')
    def delete_by_query(self, index, body, doc_type=None, params=None):
        self.producer.send(topic=self.topic,
                           value=json_util.dumps({'op': 'delete_by_query', 'index': index, 'doc_type': doc_type,
                                                  'body': body, 'params': params
                                                  }))
