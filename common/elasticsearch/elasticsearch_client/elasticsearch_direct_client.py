# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from elasticsearch.client.utils import query_params

from common.mongo.bson_c import json_util


class ElasticsearchDirectClient(Elasticsearch):
    @query_params('parent', 'pipeline', 'refresh', 'routing', 'timeout',
                  'timestamp', 'ttl', 'version', 'version_type', 'wait_for_active_shards')
    def create(self, index, doc_type, id, body, params=None):
        return Elasticsearch.create(self, index=index, doc_type=doc_type, id=id, body=json_util.dumps(body),
                                    params=params)

    @query_params('op_type', 'parent', 'pipeline', 'refresh', 'routing',
                  'timeout', 'timestamp', 'ttl', 'version', 'version_type',
                  'wait_for_active_shards')
    def index(self, index, doc_type, body, id=None, params=None):
        return Elasticsearch.index(self, index=index, doc_type=doc_type, body=json_util.dumps(body), id=id,
                                   params=params)

    @query_params('_source', '_source_exclude', '_source_include', 'fields',
                  'lang', 'parent', 'refresh', 'retry_on_conflict', 'routing', 'timeout',
                  'timestamp', 'ttl', 'version', 'version_type', 'wait_for_active_shards')
    def update(self, index, doc_type, id, body=None, params=None):
        return Elasticsearch.update(self, index=index, doc_type=doc_type, id=id, body=json_util.dumps(body),
                                    params=params)

    @query_params('parent', 'refresh', 'routing', 'timeout', 'version',
                  'version_type', 'wait_for_active_shards')
    def delete(self, index, doc_type, id, params=None):
        return Elasticsearch.delete(self, index=index, doc_type=doc_type, id=id, params=params)

    @query_params('_source', '_source_exclude', '_source_include', 'fields',
                  'pipeline', 'refresh', 'routing', 'timeout', 'wait_for_active_shards')
    def bulk(self, body, index=None, doc_type=None, params=None):
        for i, v in enumerate(body):
            body[i] = json_util.dumps(v)
        return Elasticsearch.bulk(self, body=body, index=index, doc_type=doc_type, params=params)

    @query_params('_source', '_source_exclude', '_source_include',
                  'allow_no_indices', 'analyze_wildcard', 'analyzer', 'conflicts',
                  'default_operator', 'df', 'expand_wildcards', 'from_',
                  'ignore_unavailable', 'lenient', 'pipeline', 'preference', 'q',
                  'refresh', 'request_cache', 'requests_per_second', 'routing', 'scroll',
                  'scroll_size', 'search_timeout', 'search_type', 'size', 'slices',
                  'sort', 'stats', 'terminate_after', 'timeout', 'version',
                  'version_type', 'wait_for_active_shards', 'wait_for_completion')
    def update_by_query(self, index, doc_type=None, body=None, params=None):
        return Elasticsearch.update_by_query(self, index=index, doc_type=doc_type, body=json_util.dumps(body),
                                             params=params)

    @query_params('_source', '_source_exclude', '_source_include',
                  'allow_no_indices', 'analyze_wildcard', 'analyzer', 'conflicts',
                  'default_operator', 'df', 'expand_wildcards', 'from_',
                  'ignore_unavailable', 'lenient', 'preference', 'q', 'refresh',
                  'request_cache', 'requests_per_second', 'routing', 'scroll',
                  'scroll_size', 'search_timeout', 'search_type', 'size', 'slices',
                  'sort', 'stats', 'terminate_after', 'timeout', 'version',
                  'wait_for_active_shards', 'wait_for_completion')
    def delete_by_query(self, index, body, doc_type=None, params=None):
        return Elasticsearch.delete_by_query(self, index=index, body=body, doc_type=doc_type, params=params)
