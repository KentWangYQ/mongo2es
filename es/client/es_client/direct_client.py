# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from elasticsearch.client.utils import query_params

from util.bson_c import json_util


class DirectClient(Elasticsearch):
    @query_params('_source', '_source_exclude', '_source_include', 'fields',
                  'pipeline', 'refresh', 'routing', 'timeout', 'wait_for_active_shards')
    def bulk(self, body, index=None, doc_type=None, params=None):
        return Elasticsearch.bulk(self,
                                  body=[json_util.dumps(v) for v in body],
                                  index=index,
                                  doc_type=doc_type,
                                  params=params)
