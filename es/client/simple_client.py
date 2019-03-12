# -*- coding: utf-8 -*-

import re
import copy
import es.util


class SimpleClient(object):
    def __init__(self, es_client):
        self.es_client = es_client

    def update_by_query(self, index, doc_type, data, projection, _filter, es_params, _as=None, pop_fields=None):
        _id, obj = es.util.obj_from_oplog(data=data, _filter=_filter, projection=projection, pop_fields=pop_fields)

        if not obj and not _as:
            self.es_client.delete_by_query(index=index, doc_type=doc_type, body={'query': {'term': {'_id': _id}}})
        else:
            obj = es.util.dict_projection(obj, projection)

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

                self.es_client.update_by_query(index=index, doc_type=doc_type, body=body, conflicts='proceed',
                                               params=es_params)

    def delete(self, index, doc_type, data, _filter, es_params=None):
        _id = es.util.obj_from_oplog(data, _filter)[0]
        if _id:
            self.es_client.delete_by_query(index=index, doc_type=doc_type, body={'query': {'term': {'_id': _id}}},
                                           params=es_params)
