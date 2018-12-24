# -*- coding: utf-8 -*-

import re
import copy
from elasticsearch.exceptions import ElasticsearchException

from common.log import logger
from common.elasticsearch.elasticsearch_client.elasticsearch_client import es_client
from common.mongo.bson_c import json_util

from module.elasticsearch import util


def update_by_query(index, doc_type, data, projection, _as=None, _filter={}, pop_fields=None, es_params={}):
    _id, obj = util.obj_from_oplog(data=data, _filter=_filter, projection=projection, pop_fields=pop_fields)

    if not obj and not _as:
        es_client.delete_by_query(index=index, doc_type=doc_type, body={'query': {'term': {'_id': _id}}})
    else:
        obj = util.dict_projection(obj, projection)

        if isinstance(obj, dict) and len(obj) > 0:
            _as = _as + '.' if _as else ''
            inline = ''

            if obj:
                for k in obj:
                    ks = copy.deepcopy(k)
                    for key in re.findall(r'\.\d+', k):
                        ks = ks.replace(key, '[{0}]'.format(key[1:]))

                    inline += 'ctx._source.{0}{1}=params.{2};'.format(_as, ks, k.replace('.', ''))

                for k in obj:
                    if k.find('.') > -1:
                        kr = k.replace('.', '')
                        obj[kr] = obj[k]
                        del obj[k]
            else:
                inline += 'ctx._source.{0}={};'.format(_as)

            query = {'term': {(_as + '_id'): _id}}

            body = {'query': query, 'script': {'inline': inline, 'params': obj, 'lang': 'painless'}}

            es_client.update_by_query(index=index, doc_type=doc_type, body=body, conflicts='proceed',
                                      params=es_params)


def delete(index, doc_type, data, _filter={}, es_params=None):
    _id = util.obj_from_oplog(data, _filter)[0]
    if _id:
        es_client.delete_by_query(index=index, doc_type=doc_type, body={'query': {'term': {'_id': _id}}},
                                  params=es_params)


def index_exists_create(index, mappings=None, settings=None):
    if not es_client.indices.exists(index=index):
        logger.info('index not exists! create now...\nnew mapping:')
        logger.info(json_util.dumps(mappings))

        res = es_client.indices.create(index=index,
                                       body={
                                           'mappings': mappings,
                                           'settings': settings
                                       })

        if res and res['acknowledged']:
            return 'success'
        else:
            raise ElasticsearchException(res)
