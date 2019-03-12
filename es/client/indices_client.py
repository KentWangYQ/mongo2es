# -*- coding: utf8 -*-

from elasticsearch.exceptions import ElasticsearchException
from elasticsearch.client.utils import query_params
from util.log import logger
from util.bson_c import json_util


class IndicesClient(object):
    def __init__(self, es_client):
        self.es_client = es_client

    @query_params('master_timeout', 'timeout', 'wait_for_active_shards')
    def create(self, index, body=None, params=None):
        if not self.es_client.indices.exists(index=index):
            logger.info('index "' + index + '": not exists! create now...\nnew mapping:')
            if body and body.get('mappings') is not None:
                logger.info(json_util.dumps(body.get('mappings')))

            res = self.es_client.indices.create(index=index,
                                                body=body,
                                                params=params)

            if res and res['acknowledged']:
                return 'success'
            else:
                raise ElasticsearchException(res)
